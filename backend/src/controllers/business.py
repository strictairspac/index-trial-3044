from src.services import workloads,candidates,experiments,telemetry_store,archive
from src.services.audit import log
def save_workload(data,user,item_id=None):
 row=workloads.save(data,item_id);log(user.email,'workload.save',row['name']);return row
def save_candidate(data,user,item_id=None):
 row=candidates.save(data,item_id);log(user.email,'candidate.save',row['name']);return row
def start_experiment(data,user):
 row=experiments.create(data);log(user.email,'experiment.queue',row['name']);return row
def backfill(data,user):
 row=telemetry_store.backfill(data);log(user.email,'telemetry.backfill',f"id={row['id']} source={row['source']}");return row
def correct(item_id,data,user):
 row=telemetry_store.correct(item_id,data.value,data.device_type);log(user.email,'telemetry.correct',f"id={row['id']} source={row['source']}");return row
def start_archive(data,user):
 row=archive.create(data.cutoff);log(user.email,'archive.start',f"job={row['id']} cutoff={row['cutoff']}");return row
def resume_archive(job_id,user):
 row=archive.resume(job_id);log(user.email,'archive.resume',f"job={row['id']} phase={row['phase']}");return row
