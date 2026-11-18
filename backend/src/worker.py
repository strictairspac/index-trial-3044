import time,logging,redis,threading
from pathlib import Path
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.workload import Workload
from src.models.candidate import Candidate
from src.models.experiment import Experiment
from src.models.archive import ArchiveJob
from src.services.index_algorithm import run
from src.services import archive
from src.services.audit import log
q=redis.Redis.from_url(REDIS_URL)
def run_experiment(run_id):
 with Session() as s:
  row=s.get(Experiment,run_id)
  if not row or row.status!='queued':return
  row.status='running';snapshot=row.snapshot;s.commit()
 try:
  result=run(snapshot)
  with Session() as s:r=s.get(Experiment,run_id);r.result=result;r.status='completed';s.commit()
  log('worker','experiment.completed',str(run_id))
 except Exception as exc:
  logging.exception('trial failed')
  with Session() as s:r=s.get(Experiment,run_id);r.status='failed';r.error=str(exc)[:1000];s.commit()
def run_archive(job_id):
 try:archive.execute(job_id)
 except Exception as exc:
  logging.exception('archive failed')
  with Session() as s:
   r=s.get(ArchiveJob,job_id)
   if r and r.phase not in ('completed','failed'):r.phase='failed';r.error=str(exc)[:1000];s.commit()
def dispatch(key,run_id):
 if key=='index-trials':run_experiment(run_id)
 else:run_archive(run_id)
while True:
 Path('/tmp/worker-heartbeat').touch()
 try:
  item=q.blpop(['index-trials','archive-jobs'],timeout=2)
  if not item:continue
  threading.Thread(target=dispatch,args=(item[0].decode(),int(item[1])),daemon=True).start()
 except Exception:logging.exception('worker loop');time.sleep(2)
