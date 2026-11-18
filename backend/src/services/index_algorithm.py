"""Controlled MySQL index trial. All dynamic identifiers are server-whitelisted.
Queries go through the logical telemetry store, so a hot->cold archive switch
mid-trial cannot change the record version seen before and after index creation."""
import hashlib,json,time,statistics
from sqlalchemy import text
from src.config.database import engine
from src.constants.enums import COLUMNS
from src.services.telemetry_store import logical_select,current_cutoff
TABLES=('telemetry','telemetry_archive')
def prefix_score(columns,w):
 equality={'sensor_id'}|({'device_type'} if w.get('device_type') else set())
 score=0;matched=[];reason=[]
 for col in columns:
  if col in equality:score+=40;matched.append(col);reason.append(f'{col}: equality prefix +40')
  elif col=='recorded_at':score+=20;matched.append(col);reason.append('recorded_at: range +20; following columns cannot extend lookup');break
  else:reason.append(f'{col}: unmatched prefix stops lookup');break
 return {'score':min(100,score),'matched_prefix':matched,'reason':reason,'formula':'equality prefix +40/column, first range +20, max 100; heuristic only'}
def _timed(conn,build):
 samples=[];rows=[]
 for _ in range(5):
  query,params,_=build()
  start=time.perf_counter();rows=[dict(r) for r in conn.execute(text(query),params).mappings()];samples.append((time.perf_counter()-start)*1000)
 return rows,round(statistics.median(samples),4),[round(x,4) for x in samples]
def _write_probe(conn):
 cutoff=current_cutoff(conn)
 target='telemetry_archive' if cutoff and '2026-11-17T12:00:00'<cutoff else 'telemetry'
 t=time.perf_counter()
 conn.execute(text(f"INSERT INTO {target}(sensor_id,recorded_at,device_type,value) VALUES (999,'2026-11-17T12:00:00','pump',42.5)"),[{} for _ in range(20)])
 elapsed=(time.perf_counter()-t)*1000;conn.rollback();return target,round(elapsed,4)
def _index_names(conn,table):
 return conn.execute(text("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name=:t AND index_name='trial_candidate'"),{'t':table}).scalar()
def run(snapshot):
 w=snapshot['workload'];columns=snapshot['candidate']['columns']
 if not columns or any(c not in COLUMNS for c in columns) or len(set(columns))!=len(columns):raise ValueError('invalid index columns')
 def build():return logical_select(w['sensor_id'],w['start_at'],w['end_at'],w.get('device_type'),current_cutoff(conn))
 with engine.connect() as conn:
  if conn.execute(text("SELECT GET_LOCK('index_trial_serial',30)")).scalar()!=1:raise RuntimeError('another trial holds lock')
  try:
   for table in TABLES:
    if _index_names(conn,table):conn.execute(text(f'DROP INDEX trial_candidate ON {table}'));conn.commit()
   query,params,tables=build()
   before=[dict(r) for r in conn.execute(text('EXPLAIN '+query),params).mappings()]
   rows,before_ms,before_samples=_timed(conn,build)
   probe_target,write_before=_write_probe(conn)
   ddl='CREATE INDEX trial_candidate ON {} ('+','.join('`'+c+'`' for c in columns)+')'
   for table in TABLES:conn.execute(text(ddl.format(table)))
   conn.commit()
   for table in TABLES:conn.execute(text(f'ANALYZE TABLE {table}')).all()
   after_query,after_params,after_tables=build()
   after=[dict(r) for r in conn.execute(text('EXPLAIN '+after_query),after_params).mappings()]
   result,after_ms,after_samples=_timed(conn,build)
   _,write_after=_write_probe(conn)
   canonical=lambda records:hashlib.sha256(json.dumps(records,sort_keys=True).encode()).hexdigest()
   return {'prefix':prefix_score(columns,w),'query':query,'parameters':params,'rows':len(result),'before_plan':before,'after_plan':after,'before_median_ms':before_ms,'after_median_ms':after_ms,'before_samples_ms':before_samples,'after_samples_ms':after_samples,'insert20_before_ms':write_before,'insert20_after_ms':write_after,'equal_results':canonical(rows)==canonical(result),'checksum':canonical(result),'preview':result[:10],'archive_cutoff':current_cutoff(conn),'tables':sorted(set(tables)|set(after_tables)),'probe_target':probe_target,'limitations':['5000-row deterministic dataset; timings include Python driver and cache effects','20-row write probe is rolled back; sequence gaps are normal','heuristic prefix score is not MySQL optimizer cost; no guaranteed speedup','queries route across hot/cold tables by the archived cutoff; a mid-trial switch cannot change the record version']}
  finally:
   conn.rollback()
   try:
    for table in TABLES:
     if _index_names(conn,table):conn.execute(text(f'DROP INDEX trial_candidate ON {table}'));conn.commit()
   finally:conn.execute(text("SELECT RELEASE_LOCK('index_trial_serial')"));conn.commit()
