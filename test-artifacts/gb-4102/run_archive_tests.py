#!/usr/bin/env python3
"""Hot/cold archive acceptance tests (gb-4102).

Scenario A: corrections and backfills confirmed DURING a migration are preserved.
Scenario B: killing the worker before the switch, then resuming, does not duplicate rows.
Scenario C: a cross hot/cold index trial reads the same record version across a switch.

Runs against the live stack (backend API + real MySQL + Redis + its own worker).
Usage: python3 run_archive_tests.py  (env: API_URL, DB_DSN_* , REDIS_URL, WORKER_ENV_*)
"""
import json, os, signal, subprocess, sys, time, hashlib
import httpx
import pymysql

API = os.getenv('API_URL', 'http://127.0.0.1:19241')
DB = dict(host=os.getenv('DB_HOST', '127.0.0.1'), port=int(os.getenv('DB_PORT', '17741')),
          user=os.getenv('DB_USER', 'trial'), password=os.getenv('DB_PASS', 'trialpass'),
          database=os.getenv('DB_NAME', 'indextrial'), autocommit=True)
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'backend'))
ENV = dict(os.environ, DATABASE_URL=f"mysql+pymysql://{DB['user']}:{DB['password']}@{DB['host']}:{DB['port']}/{DB['database']}",
           REDIS_URL=os.getenv('REDIS_URL', 'redis://127.0.0.1:16379/0'), JWT_SECRET='local-test-secret',
           ARCHIVE_BATCH_SIZE=os.getenv('ARCHIVE_BATCH_SIZE', '100'), ARCHIVE_THROTTLE_MS=os.getenv('ARCHIVE_THROTTLE_MS', '150'))
C1, C2, C3 = '2026-11-15T00:00:00', '2026-11-20T00:00:00', '2026-11-25T00:00:00'
results, evidence = [], {}

def check(name, ok, detail=''):
    results.append({'check': name, 'ok': bool(ok), 'detail': str(detail)[:300]})
    print(('PASS' if ok else 'FAIL'), '-', name, ('| ' + str(detail)[:160] if detail else ''))

def db(sql, args=None, one=False):
    with pymysql.connect(**DB) as c:
        with c.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args or ())
            rows = cur.fetchall()
    return (rows[0] if rows else None) if one else rows

def scalar(sql, args=None):
    row = db(sql, args, one=True)
    return list(row.values())[0] if row else None

class Api:
    def __init__(self):
        self.c = httpx.Client(base_url=API, timeout=15)
        r = self.c.post('/auth/login', json={'email': 'operator@index.local', 'password': 'Trial2026!'})
        r.raise_for_status()
        self.c.headers['Authorization'] = 'Bearer ' + r.json()['token']
    def __getattr__(self, m):
        return lambda p, **kw: self._call(m, p, **kw)
    def _call(self, m, p, **kw):
        r = self.c.request(m.upper(), p, **kw)
        if r.status_code >= 400:
            raise AssertionError(f'{m} {p} -> {r.status_code} {r.text[:200]}')
        return r.json()

def wait_job(api, want, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = api.get('/archive')
        j = s['jobs'][0] if s['jobs'] else None
        if j and want(j):
            return s, j
        time.sleep(0.1)
    raise TimeoutError(f'archive job did not reach expected state in {timeout}s')

def wait_experiment(api, exp_id, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        e = api.get(f'/experiments/{exp_id}')
        if e['status'] in ('completed', 'failed'):
            return e
        time.sleep(0.3)
    raise TimeoutError('experiment did not finish')

def logical_rows(sensor, start, end):
    """Straight from the tables, mirroring the logical read: cold below cutoff + hot at/above."""
    cutoff = scalar('SELECT cutoff FROM archive_state WHERE id=1')
    rows = []
    if cutoff and start < cutoff:
        rows += db('SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry_archive WHERE sensor_id=%s AND recorded_at>=%s AND recorded_at<%s', (sensor, start, min(end, cutoff)))
    lo = cutoff if cutoff and start < cutoff else start
    if end >= lo:
        rows += db('SELECT id,sensor_id,recorded_at,device_type,value FROM telemetry WHERE sensor_id=%s AND recorded_at>=%s AND recorded_at<=%s', (sensor, lo, end))
    return sorted(rows, key=lambda r: r['id'])

def canonical(rows):
    return hashlib.sha256(json.dumps([{k: r[k] for k in ('id', 'sensor_id', 'recorded_at', 'device_type', 'value')} for r in rows], sort_keys=True).encode()).hexdigest()

def start_worker():
    return subprocess.Popen([sys.executable, '-m', 'src.worker'], cwd=BACKEND_DIR, env=ENV,
                            stdout=open('/tmp/gb4102-worker.log', 'ab'), stderr=subprocess.STDOUT)

def reset():
    subprocess.run([sys.executable, '-c', f"""
import pymysql
c=pymysql.connect(host='{DB['host']}',port={DB['port']},user='root',password='',unix_socket='/tmp/devenv/mysql.sock',autocommit=True)
cur=c.cursor()
cur.execute('DROP DATABASE IF EXISTS {DB['database']}')
cur.execute("CREATE DATABASE {DB['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
"""], check=True)
    subprocess.run([sys.executable, '-m', 'scripts.seed_demo'], cwd=BACKEND_DIR, env=ENV, check=True, capture_output=True)
    subprocess.run(['/tmp/devenv/redis-7.2.5/src/redis-cli', '-p', '16379', 'flushdb'], check=True, capture_output=True)

def main():
    print('== reset database, flush queue, restart worker ==')
    reset()
    subprocess.run(['pkill', '-9', '-f', 'src.worker'], capture_output=True)
    time.sleep(0.5)
    worker = start_worker()
    api = Api()
    try:
        # ---------- Scenario A: correction + backfill during migration ----------
        print('\n== Scenario A: 搬迁时更正与补录 ==')
        before_hot = scalar('SELECT COUNT(*) FROM telemetry')
        job = api.post('/archive/jobs', json={'cutoff': C1})
        check('A1 任务创建进入复制阶段', job['phase'] == 'copying' and job['total_rows'] > 0, f"job={job['id']} total={job['total_rows']}")
        _, copying = wait_job(api, lambda j: j['phase'] == 'copying' and j['copied_rows'] > 0)
        check('A2 复制进行中可观察到进度', 0 < copying['copied_rows'] < job['total_rows'], f"copied={copying['copied_rows']}/{job['total_rows']}")
        r = api.put('/telemetry/100', json={'value': 777.77})
        check('A3 搬迁中更正热表旧记录被接受', r['source'] == 'hot', r)
        r = api.post('/telemetry', json={'sensor_id': 17, 'recorded_at': '2026-11-10T08:00:00', 'device_type': 'pump', 'value': 55.5})
        backfill_old = r['id']
        check('A4 搬迁中补录截止前记录被接受', r['source'] == 'hot', r)
        r = api.post('/telemetry', json={'sensor_id': 17, 'recorded_at': '2026-11-20T08:00:00', 'device_type': 'pump', 'value': 66.6})
        backfill_new = r['id']
        check('A5 搬迁中补录截止后记录被接受', r['source'] == 'hot', r)
        s, done = wait_job(api, lambda j: j['phase'] == 'completed')
        check('A6 归档任务完成且已切换', s['state'] == 'archived' and s['cutoff'] == C1, f"state={s['state']} cutoff={s['cutoff']}")
        row = db('SELECT value FROM telemetry_archive WHERE id=100', one=True)
        check('A7 搬迁期间的更正保留在冷表', row and abs(row['value'] - 777.77) < 1e-6, row)
        row = db('SELECT id,value FROM telemetry_archive WHERE sensor_id=17 AND recorded_at=%s', ('2026-11-10T08:00:00',), one=True)
        check('A8 截止前补录随搬迁进入冷表', row and row['id'] == backfill_old and abs(row['value'] - 55.5) < 1e-6, row)
        row = db('SELECT id,value FROM telemetry WHERE sensor_id=17 AND recorded_at=%s', ('2026-11-20T08:00:00',), one=True)
        check('A9 截止后补录留在热表', row and row['id'] == backfill_new, row)
        check('A10 热表旧副本已清理', scalar('SELECT COUNT(*) FROM telemetry WHERE recorded_at<%s', (C1,)) == 0)
        check('A11 冷热合计等于原数据加两次补录', scalar('SELECT COUNT(*) FROM telemetry') + scalar('SELECT COUNT(*) FROM telemetry_archive') == before_hot + 2,
              f"hot={scalar('SELECT COUNT(*) FROM telemetry')} cold={scalar('SELECT COUNT(*) FROM telemetry_archive')}")
        check('A12 冷表无重复记录', scalar('SELECT COUNT(*) FROM telemetry_archive') == scalar('SELECT COUNT(DISTINCT id) FROM telemetry_archive'))
        listed = api.get('/telemetry', params={'sensor_id': 17, 'start': '2026-11-01T00:00:00', 'end': '2026-11-30T23:59:59', 'limit': 200})
        listed100 = api.get('/telemetry', params={'sensor_id': 100, 'start': '2026-11-01T00:00:00', 'end': '2026-11-30T23:59:59', 'limit': 200})
        check('A13 逻辑读跨表可见补录与更正', any(r['id'] == backfill_old and r['source'] == 'cold' for r in listed)
              and any(r['id'] == backfill_new and r['source'] == 'hot' for r in listed)
              and any(r['id'] == 100 and abs(r['value'] - 777.77) < 1e-6 and r['source'] == 'cold' for r in listed100), f'rows={len(listed)}')
        evidence['A'] = {'job': done, 'backfill_old': backfill_old, 'backfill_new': backfill_new}

        # ---------- Scenario B: interrupt before switch, resume ----------
        print('\n== Scenario B: 切换前中断续跑 ==')
        expect_migrate = scalar('SELECT COUNT(*) FROM telemetry WHERE recorded_at<%s', (C2,))
        cold_before = scalar('SELECT COUNT(*) FROM telemetry_archive')
        job = api.post('/archive/jobs', json={'cutoff': C2})
        _, copying = wait_job(api, lambda j: j['phase'] == 'copying' and 0 < j['copied_rows'] < j['total_rows'])
        worker.kill()  # SIGKILL: real crash before the switch
        worker.wait()
        check('B1 worker 在切换前被杀死', copying['phase'] == 'copying', f"killed at copied={copying['copied_rows']}/{copying['total_rows']}")
        time.sleep(0.5)
        s = api.get('/archive')
        check('B2 中断后未发生切换', s['cutoff'] == C1 and s['jobs'][0]['phase'] == 'copying' and not s['jobs'][0]['switched_at'],
              f"cutoff={s['cutoff']} phase={s['jobs'][0]['phase']}")
        cold_mid = scalar('SELECT COUNT(*) FROM telemetry_archive')
        check('B3 中断时冷表只有部分副本', cold_before < cold_mid < cold_before + expect_migrate, f'cold={cold_mid}')
        worker = start_worker()
        time.sleep(1.5)
        r = api.post(f"/archive/jobs/{job['id']}/resume")
        check('B4 续跑请求被接受', r['id'] == job['id'], r['phase'])
        s, done = wait_job(api, lambda j: j['phase'] == 'completed')
        check('B5 续跑后完成并切换', s['cutoff'] == C2 and done['switched_at'], f"cutoff={s['cutoff']}")
        cold_now = scalar('SELECT COUNT(*) FROM telemetry_archive')
        check('B6 续跑不重复搬迁', cold_now == cold_before + expect_migrate and cold_now == scalar('SELECT COUNT(DISTINCT id) FROM telemetry_archive'),
              f'cold={cold_now} expect={cold_before + expect_migrate}')
        check('B7 热表新边界以下已清理', scalar('SELECT COUNT(*) FROM telemetry WHERE recorded_at<%s', (C2,)) == 0)
        check('B8 场景A的更正仍在冷表', abs(scalar('SELECT value FROM telemetry_archive WHERE id=100') - 777.77) < 1e-6)
        check('B9 冷表越界记录为零', scalar('SELECT COUNT(*) FROM telemetry_archive WHERE recorded_at>=%s', (C2,)) == 0)
        evidence['B'] = {'job': done, 'cold_mid_interrupt': cold_mid, 'cold_final': cold_now}

        # ---------- Scenario C: cross hot/cold trial across a switch ----------
        print('\n== Scenario C: 跨冷热索引试验遇到切换 ==')
        worker.kill()
        worker.wait()
        fast_env = dict(ENV, ARCHIVE_BATCH_SIZE='1000', ARCHIVE_THROTTLE_MS='0')
        worker = subprocess.Popen([sys.executable, '-m', 'src.worker'], cwd=BACKEND_DIR, env=fast_env,
                                  stdout=open('/tmp/gb4102-worker.log', 'ab'), stderr=subprocess.STDOUT)
        time.sleep(1)
        expected_rows = logical_rows(17, '2026-11-01T00:00:00', '2026-11-30T23:59:59')
        expected_sha = canonical(expected_rows)
        mid_switch = None
        for i, cutoff in enumerate(['2026-11-22T00:00:00', '2026-11-23T00:00:00', '2026-11-24T00:00:00']):
            exp = api.post('/experiments', json={'name': f'gb4102-cross-{i}', 'candidate_id': 1})
            job = api.post('/archive/jobs', json={'cutoff': cutoff})
            e = wait_experiment(api, exp['id'])
            s, done = wait_job(api, lambda j: j['phase'] == 'completed')
            check(f'C{i+1}1 切换期间试验完成', e['status'] == 'completed', e.get('error'))
            r = e['result']
            check(f'C{i+1}2 建索引前后结果等价且行数不变', r['equal_results'] is True and r['rows'] == len(expected_rows))
            check(f'C{i+1}3 试验读取跨越冷热两表', set(r['tables']) == {'telemetry', 'telemetry_archive'} and 'UNION' in r['query'], r['tables'])
            check(f'C{i+1}4 校验和与切换前逻辑数据集一致', r['checksum'] == expected_sha, f"trial={r['checksum'][:12]} expect={expected_sha[:12]}")
            if r['parameters'].get('cutoff') != r['archive_cutoff']:
                mid_switch = {'experiment': e['id'], 'before_cutoff': r['parameters'].get('cutoff'), 'end_cutoff': r['archive_cutoff'], 'checksum': r['checksum']}
                break
        check('C5 切换发生在单次试验执行窗口内', mid_switch is not None, mid_switch)
        after_rows = logical_rows(17, '2026-11-01T00:00:00', '2026-11-30T23:59:59')
        check('C6 切换前后逻辑读逐行一致', canonical(after_rows) == expected_sha and len(after_rows) == len(expected_rows))
        exp2 = api.post('/experiments', json={'name': 'gb4102-post-switch', 'candidate_id': 1})
        e2 = wait_experiment(api, exp2['id'])
        check('C7 切换后复跑试验仍读同一版记录', e2['status'] == 'completed' and e2['result']['checksum'] == expected_sha and e2['result']['equal_results'] is True,
              e2['result']['checksum'][:12] if e2.get('result') else e2.get('error'))
        evidence['C'] = {'expected_sha': expected_sha, 'rows': len(expected_rows), 'mid_switch': mid_switch,
                         'post_switch': {'id': e2['id'], 'checksum': e2['result']['checksum']}}
    finally:
        worker.terminate()
    ok = all(x['ok'] for x in results)
    report = {'passed': ok, 'checks': results, 'evidence': evidence}
    out = os.path.join(os.path.dirname(__file__), 'archive-test-report.json')
    with open(out, 'w') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n{'ALL PASS' if ok else 'FAILURES'}: {sum(1 for x in results if x['ok'])}/{len(results)} checks -> {out}")
    sys.exit(0 if ok else 1)

if __name__ == '__main__':
    main()
