"""Logical telemetry store: routes reads/writes across hot (telemetry) and cold
(telemetry_archive) tables by the archived cutoff. Invariants:
- before a job switches, hot holds every confirmed write; cold is a synced copy;
- after the switch, rows below the cutoff live only in cold, the rest only in hot;
- during copying/verifying, writes below the job cutoff are dual-written to cold,
  and every sync into cold is a conditional upsert keyed on updated_at so a later
  correction always wins regardless of arrival order."""
from fastapi import HTTPException
from sqlalchemy import text
from src.config.database import engine
COLS='id, sensor_id, recorded_at, device_type, value'
def upsert_from_hot(where):
 """Conditional hot->cold sync: a row is overwritten only by a version with a
 newer-or-equal updated_at, so a confirmed correction always wins the race."""
 return f"""INSERT INTO telemetry_archive (id, sensor_id, recorded_at, device_type, value, updated_at)
 SELECT id, sensor_id, recorded_at, device_type, value, updated_at FROM telemetry WHERE {where}
 ON DUPLICATE KEY UPDATE
  device_type=IF(VALUES(updated_at)>=telemetry_archive.updated_at,VALUES(device_type),telemetry_archive.device_type),
  value=IF(VALUES(updated_at)>=telemetry_archive.updated_at,VALUES(value),telemetry_archive.value),
  updated_at=IF(VALUES(updated_at)>=telemetry_archive.updated_at,VALUES(updated_at),telemetry_archive.updated_at)"""
UPSERT_BY_ID=upsert_from_hot('id=:id')
def state(conn):
 row=conn.execute(text('SELECT cutoff FROM archive_state WHERE id=1')).first()
 job=conn.execute(text("SELECT id,cutoff FROM archive_jobs WHERE phase IN ('copying','verifying') ORDER BY id DESC LIMIT 1")).first()
 return {'cutoff':row[0] if row else None,'active_job':dict(job._mapping) if job else None}
def current_cutoff(conn=None):
 if conn:return state(conn)['cutoff']
 with engine.connect() as c:return state(c)['cutoff']
def logical_select(sensor_id,start,end,device_type,cutoff):
 """Build the cutover-transparent SELECT for a sensor/time range. Returns (sql, params, tables)."""
 params={'sensor':sensor_id,'start':start,'end':end}
 dev=''
 if device_type:dev=' AND device_type=:device';params['device']=device_type
 if cutoff and start<cutoff:
  params['cutoff']=cutoff
  cold=f"SELECT {COLS} FROM telemetry_archive WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<:cutoff{dev}"
  if end<cutoff:return cold+' ORDER BY id',params,['telemetry_archive']
  hot=f"SELECT {COLS} FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:cutoff AND recorded_at<=:end{dev}"
  return f"SELECT * FROM ({cold} UNION ALL {hot}) AS merged ORDER BY id",params,['telemetry_archive','telemetry']
 return f"SELECT {COLS} FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end{dev} ORDER BY id",params,['telemetry']
def list_rows(sensor_id=None,start=None,end=None,limit=50):
 """Browse the logical dataset, newest first, annotated with the table each row is read from."""
 with engine.connect() as conn:
  cutoff=current_cutoff(conn)
  params={'limit':limit}
  if sensor_id and start and end:
   params.update({'sensor':sensor_id,'start':start,'end':end})
   hot=f"SELECT {COLS}, 'hot' AS source FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<=:end"
   if cutoff and start<cutoff:
    params['cutoff']=cutoff
    cold=f"SELECT {COLS}, 'cold' AS source FROM telemetry_archive WHERE sensor_id=:sensor AND recorded_at>=:start AND recorded_at<:cutoff"
    if end<cutoff:parts=[cold]
    else:parts=[cold,f"SELECT {COLS}, 'hot' AS source FROM telemetry WHERE sensor_id=:sensor AND recorded_at>=:cutoff AND recorded_at<=:end"]
   else:parts=[hot]
  else:
   parts=[f"SELECT {COLS}, 'hot' AS source FROM telemetry"]
   if cutoff:parts.append(f"SELECT {COLS}, 'cold' AS source FROM telemetry_archive")
  sql='SELECT * FROM ('+' UNION ALL '.join(parts)+') AS merged ORDER BY id DESC LIMIT :limit'
  return [dict(r) for r in conn.execute(text(sql),params).mappings()]
def backfill(data):
 """Insert one confirmed row. Routed to cold when it falls below the switched cutoff;
 dual-written when an unfinished job covers its timestamp."""
 with engine.begin() as conn:
  st=state(conn);cutoff=st['cutoff'];job=st['active_job']
  if cutoff and data.recorded_at<cutoff:
   r=conn.execute(text('INSERT INTO telemetry_archive (sensor_id,recorded_at,device_type,value) VALUES (:sensor,:at,:device,:value)'),{'sensor':data.sensor_id,'at':data.recorded_at,'device':data.device_type,'value':data.value})
   return {'id':r.lastrowid,'source':'cold'}
  r=conn.execute(text('INSERT INTO telemetry (sensor_id,recorded_at,device_type,value) VALUES (:sensor,:at,:device,:value)'),{'sensor':data.sensor_id,'at':data.recorded_at,'device':data.device_type,'value':data.value})
  new_id=r.lastrowid
  if job and data.recorded_at<job['cutoff']:
   conn.execute(text(UPSERT_BY_ID),{'id':new_id})
  return {'id':new_id,'source':'hot'}
def correct(item_id,value,device_type):
 """Correct value/device_type of one logical record. Identity fields (sensor, time) stay immutable."""
 sets='value=:value'+(', device_type=:device' if device_type else '')
 params={'id':item_id,'value':value,'device':device_type}
 with engine.begin() as conn:
  st=state(conn);cutoff=st['cutoff'];job=st['active_job']
  if cutoff and conn.execute(text('SELECT 1 FROM telemetry_archive WHERE id=:id'),{'id':item_id}).first():
   conn.execute(text(f'UPDATE telemetry_archive SET {sets}, updated_at=CURRENT_TIMESTAMP(6) WHERE id=:id'),params)
   return {'id':item_id,'source':'cold'}
  row=conn.execute(text('SELECT recorded_at FROM telemetry WHERE id=:id'),{'id':item_id}).first()
  if not row:raise HTTPException(404,'未找到该遥测记录')
  conn.execute(text(f'UPDATE telemetry SET {sets} WHERE id=:id'),params)
  if job and row[0]<job['cutoff']:
   conn.execute(text(UPSERT_BY_ID),{'id':item_id})
  return {'id':item_id,'source':'hot'}
