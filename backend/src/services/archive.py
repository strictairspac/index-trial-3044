"""Archive engine: copy -> verify -> atomic switch -> clean. Every step is idempotent
so an interrupted job resumes without duplicating rows; the read/write location flips
in a single transaction only after hot and cold checksums match."""
import time,logging,os
import redis
from fastapi import HTTPException
from sqlalchemy import text,bindparam
from src.config.database import engine,Session
from src.config.settings import REDIS_URL
from src.constants.enums import COLD_NATIVE_FLOOR
from src.models.archive import ArchiveJob,ArchiveState
from src.services.telemetry_store import state,upsert_from_hot
from src.services.audit import log
from src.utils.serialization import record
queue=redis.Redis.from_url(REDIS_URL)
BATCH_SIZE=int(os.getenv('ARCHIVE_BATCH_SIZE','500'))
THROTTLE_MS=int(os.getenv('ARCHIVE_THROTTLE_MS','50'))
SYNC_IDS=upsert_from_hot('id IN :ids')
CATCHUP=upsert_from_hot('recorded_at < :cutoff AND updated_at > :since')
def _checksum(conn,table,lo,hi,migrated_only):
 sql=f"SELECT COUNT(*) c, COALESCE(SUM(CRC32(CONCAT_WS('#',id,sensor_id,recorded_at,device_type,value,updated_at))),0) h FROM {table} WHERE recorded_at>=:lo AND recorded_at<:hi"
 if migrated_only:sql+=f' AND id < {COLD_NATIVE_FLOOR}'
 row=conn.execute(text(sql),{'lo':lo,'hi':hi}).first()
 return row.c,int(row.h)
def _phase(job_id):
 with engine.connect() as conn:return conn.execute(text('SELECT phase FROM archive_jobs WHERE id=:j'),{'j':job_id}).scalar()
def _copy(job_id,cutoff,batch_size,throttle_ms):
 while True:
  with engine.begin() as conn:
   ck=conn.execute(text('SELECT checkpoint_id FROM archive_jobs WHERE id=:j'),{'j':job_id}).scalar()
   ids=[r[0] for r in conn.execute(text('SELECT id FROM telemetry WHERE recorded_at<:c AND id>:ck ORDER BY id LIMIT :b'),{'c':cutoff,'ck':ck,'b':batch_size})]
   if ids:
    conn.execute(text(SYNC_IDS).bindparams(bindparam('ids',expanding=True)),{'ids':ids})
    conn.execute(text('UPDATE archive_jobs SET checkpoint_id=:ck, copied_rows=copied_rows+:n WHERE id=:j'),{'ck':max(ids),'n':len(ids),'j':job_id})
  if not ids:break
  if throttle_ms:time.sleep(throttle_ms/1000)
 with engine.begin() as conn:conn.execute(text("UPDATE archive_jobs SET phase='verifying' WHERE id=:j AND phase='copying'"),{'j':job_id})
def _verify_and_switch(job_id,cutoff,prev):
 lo=prev or ''
 with engine.connect() as conn:since=conn.execute(text('SELECT created_at FROM archive_jobs WHERE id=:j'),{'j':job_id}).scalar()
 for attempt in range(1,6):
  with engine.begin() as conn:
   conn.execute(text(CATCHUP),{'cutoff':cutoff,'since':since})
   hot=_checksum(conn,'telemetry',lo,cutoff,False)
   cold=_checksum(conn,'telemetry_archive',lo,cutoff,True)
   conn.execute(text('UPDATE archive_jobs SET verify_rounds=verify_rounds+1 WHERE id=:j'),{'j':job_id})
   if hot==cold:
    conn.execute(text('UPDATE archive_state SET cutoff=:c, updated_at=CURRENT_TIMESTAMP(6) WHERE id=1'),{'c':cutoff})
    conn.execute(text("UPDATE archive_jobs SET phase='cleaning', switched_at=CURRENT_TIMESTAMP(6) WHERE id=:j"),{'j':job_id})
    log('worker','archive.switch',f'job={job_id} cutoff={cutoff} rows={hot[0]}')
    return
  time.sleep(0.2)
 raise RuntimeError('核对 5 轮后热冷表仍不一致，任务保留待重试')
def _clean(job_id,cutoff,batch_size,throttle_ms):
 while True:
  with engine.connect() as conn:ids=[r[0] for r in conn.execute(text('SELECT id FROM telemetry WHERE recorded_at<:c ORDER BY id LIMIT :b'),{'c':cutoff,'b':batch_size})]
  if not ids:break
  with engine.begin() as conn:conn.execute(text(SYNC_IDS).bindparams(bindparam('ids',expanding=True)),{'ids':ids})
  with engine.begin() as conn:
   conn.execute(text('DELETE FROM telemetry WHERE id IN :ids').bindparams(bindparam('ids',expanding=True)),{'ids':ids})
   conn.execute(text('UPDATE archive_jobs SET cleaned_rows=cleaned_rows+:n WHERE id=:j'),{'n':len(ids),'j':job_id})
  if throttle_ms:time.sleep(throttle_ms/1000)
 with engine.begin() as conn:conn.execute(text("UPDATE archive_jobs SET phase='completed', finished_at=CURRENT_TIMESTAMP(6) WHERE id=:j"),{'j':job_id})
 log('worker','archive.completed',f'job={job_id} cutoff={cutoff}')
def execute(job_id,batch_size=None,throttle_ms=None):
 """Run a job to completion from its persisted phase; safe to re-enter after a crash."""
 batch_size=batch_size or BATCH_SIZE;throttle_ms=THROTTLE_MS if throttle_ms is None else throttle_ms
 lock=engine.connect()
 try:
  if lock.execute(text("SELECT GET_LOCK(:name,0)"),{'name':f'archive_job_{job_id}'}).scalar()!=1:return
  try:
   with Session() as s:
    job=s.get(ArchiveJob,job_id)
    if not job or job.phase in ('completed','failed'):return
    cutoff,prev=job.cutoff,job.prev_cutoff
   if _phase(job_id)=='copying':_copy(job_id,cutoff,batch_size,throttle_ms)
   if _phase(job_id)=='verifying':_verify_and_switch(job_id,cutoff,prev)
   if _phase(job_id)=='cleaning':_clean(job_id,cutoff,batch_size,throttle_ms)
  finally:lock.execute(text('SELECT RELEASE_LOCK(:name)'),{'name':f'archive_job_{job_id}'})
 finally:lock.close()
def create(cutoff):
 with engine.begin() as conn:
  st=state(conn)
  if st['active_job'] or conn.execute(text("SELECT 1 FROM archive_jobs WHERE phase IN ('copying','verifying','cleaning') LIMIT 1")).first():raise HTTPException(409,'已有归档任务在进行中，请等待完成或续跑')
  current=st['cutoff']
  if current and cutoff<=current:raise HTTPException(400,'截止时间必须晚于当前已归档边界')
  total=conn.execute(text('SELECT COUNT(*) FROM telemetry WHERE recorded_at<:c'),{'c':cutoff}).scalar()
  job_id=conn.execute(text("INSERT INTO archive_jobs (cutoff,prev_cutoff,phase,total_rows) VALUES (:c,:p,'copying',:t)"),{'c':cutoff,'p':current,'t':total}).lastrowid
 try:queue.rpush('archive-jobs',job_id)
 except redis.RedisError:
  with engine.begin() as conn:conn.execute(text("UPDATE archive_jobs SET phase='failed', error='队列不可用' WHERE id=:j"),{'j':job_id})
  raise HTTPException(503,'队列不可用，请稍后重新发起')
 return detail(job_id)
def resume(job_id):
 with Session() as s:
  job=s.get(ArchiveJob,job_id)
  if not job:raise HTTPException(404,'未找到归档任务')
  if job.phase=='completed':raise HTTPException(409,'任务已完成，无需续跑')
  if job.phase=='failed':
   job.phase='cleaning' if job.switched_at else 'copying';job.error=None;s.commit()
 try:queue.rpush('archive-jobs',job_id)
 except redis.RedisError:raise HTTPException(503,'队列不可用，请稍后重新续跑')
 return detail(job_id)
def detail(job_id):
 with Session() as s:
  job=s.get(ArchiveJob,job_id)
  if not job:raise HTTPException(404,'未找到归档任务')
  return record(job)
def status():
 with Session() as s:
  st=s.get(ArchiveState,1);cutoff=st.cutoff if st else None
  jobs=[record(j) for j in s.query(ArchiveJob).order_by(ArchiveJob.id.desc()).limit(10).all()]
 active=next((j for j in jobs if j['phase'] in ('copying','verifying')),None)
 return {'state':'migrating' if active else ('archived' if cutoff else 'idle'),'cutoff':cutoff,'active_job':active,'jobs':jobs}
