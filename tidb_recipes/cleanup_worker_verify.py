#!/usr/bin/env python3
"""CLM-01 / CLM-02 integration runner. Requires an isolated test TiDB deployment.

Dependencies: requests, mysql CLI.  Run from the corresponding Ansible playbooks.
The VDEV JSON fixture and infrastructure fixture come from the existing Holon repo.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

import requests


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def unwrap(response):
    data = response
    for _ in range(3):
        if isinstance(data, dict) and isinstance(data.get('payload'), dict):
            data = data['payload']
        elif isinstance(data, dict) and isinstance(data.get('data'), dict):
            data = data['data']
        else:
            break
    return data


class API:
    def __init__(self, base, token=None):
        self.base = base.rstrip('/')
        self.token = token

    def call(self, method, path, params=None, body=None, allow_failure=False):
        headers = {'Content-Type': 'application/json'}
        if self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        r = requests.request(method, self.base + path, headers=headers,
                             params=params, json=body, timeout=30)
        try:
            data = r.json()
        except ValueError:
            raise AssertionError(f'{method} {path}: non-JSON HTTP {r.status_code}: {r.text[:400]}')
        if not allow_failure:
            require(r.status_code == 200 or r.status_code == 201,
                    f'{method} {path}: HTTP {r.status_code}: {str(data)[:500]}')
            # Some endpoints use application status even on HTTP 200; do not guess
            # status convention, but reject explicit errors.
            require(not (isinstance(data, dict) and data.get('error')),
                    f'{method} {path}: application error: {data}')
        return r.status_code, data

    def login(self, user, password, tenant_uuid=None):
        body = {'username': user, 'password': password}
        if tenant_uuid:
            body['tenant_uuid'] = tenant_uuid
        _, raw = self.call('POST', '/users/login', body=body)
        token = unwrap(raw).get('access_token')
        require(token, f'login token absent for tenant {tenant_uuid}')
        self.token = token
        return self


def sql(args, schema, query):
    require(re.fullmatch(r'[A-Za-z0-9_]+', schema) is not None, f'Unsafe schema {schema!r}')
    cmd = ['mysql', '--protocol=TCP', '-h', args.mysql_host, '-P', str(args.mysql_port),
           '-u', args.mysql_user, '--batch', '--raw', '--skip-column-names', schema, '-e', query]
    env = os.environ.copy()
    if args.mysql_password:
        env['MYSQL_PWD'] = args.mysql_password
    p = subprocess.run(cmd, env=env, text=True, capture_output=True)
    require(p.returncode == 0, f'TiDB SQL failed for {schema}: {p.stderr.strip()}')
    return [line.split('\t') for line in p.stdout.splitlines() if line.strip()]


def sql_literal(value):
    return "'" + str(value).replace('\\', '\\\\').replace("'", "''") + "'"


def jobs_for(args, schema, vdev_id):
    return sql(args, schema, 'SELECT id,job_type,resource_id,status,COALESCE(error_msg,\'\'),' 
               'IF(completed_at IS NULL,0,1) FROM cp_cleanup_job '
               'WHERE resource_id = ' + sql_literal(vdev_id) + ' ORDER BY id')


def resources(api):
    _, result = api.call('GET', '/api/resource', params={'type': 'nisd'})
    values = unwrap(result).get('resources')
    if isinstance(values, dict):
        # Normalise common map representations, but never invent capacity.
        values = list(values.values())
    require(isinstance(values, list), 'Expected resources list from /api/resource?type=nisd')
    out = {}
    for entry in values:
        require(isinstance(entry, dict), f'Bad NISD resource: {entry!r}')
        if 'id' in entry and 'available_size' in entry:
            out[str(entry['id'])] = int(entry['available_size'])
    require(out, 'NISD resource API did not return ID/available_size values')
    return out


def placements(api, vdev_id):
    ids = set()
    index = 0
    for _ in range(10000):
        _, raw = api.call('GET', '/api/chunks', params={
            'vdev_id': vdev_id, 'start_chunk_idx': index, 'limit': 1000})
        page = unwrap(raw)
        chunks = page.get('chunks', [])
        require(isinstance(chunks, list), 'Invalid chunks page')
        for chunk in chunks:
            ids.update(str(n) for n in chunk.get('nisd_ids', []))
        if not page.get('has_more'):
            break
        nxt = page.get('next_start_chunk_idx')
        require(nxt is not None and int(nxt) > index, 'Chunk pagination did not advance')
        index = int(nxt)
    else:
        raise AssertionError('Chunk pagination limit exceeded')
    require(ids, f'No NISD placement for {vdev_id}')
    return ids


def events(api, vdev_id):
    _, raw = api.call('GET', '/api/events', params={'vdev_id': vdev_id, 'action': 1})
    items = unwrap(raw).get('events')
    require(isinstance(items, list), f'Expected events array for {vdev_id}')
    return items


def queue(api):
    _, raw = api.call('GET', '/api/cleanup-jobs')
    data = unwrap(raw)
    for name in ('pending', 'processing', 'failed'):
        require(name in data, f'Cleanup queue response missing {name}: {data}')
    return data


def create_tenant(admin, label):
    _, raw = admin.call('POST', '/tenants', body={'display_name': label})
    payload = unwrap(raw)
    tenant = payload.get('tenant', {})
    tid = tenant.get('tenant_uuid')
    schema = tenant.get('schema_name')
    username = payload.get('admin_username')
    password = payload.get('admin_password')
    require(tid and schema and username and password,
            f'Tenant create must return tenant uuid, schema, admin creds; got {payload}')
    api = API(admin.base).login(username, password, tid)
    return {'id': tid, 'schema': schema, 'api': api, 'username': username, 'password': password, 'vdevs': []}


def ensure_infra(tenant, fixture):
    _, raw = tenant['api'].call('GET', '/api/resource', params={'type': 'nisd'})
    values = unwrap(raw).get('resources')
    if values:
        return
    require(fixture, 'No infrastructure found and --infra-json not provided')
    tenant['api'].call('POST', '/api/infra', body=json.loads(Path(fixture).read_text()))
    resources(tenant['api'])


def create_vdev(tenant, fixture, serial):
    body = json.loads(Path(fixture).read_text())
    body['name'] = f"clm-{serial}-{uuid.uuid4().hex[:8]}"
    _, raw = tenant['api'].call('POST', '/api/vdev', body=body)
    vdev_id = unwrap(raw).get('vdev_id')
    require(vdev_id, f'Create VDEV missing ID: {raw}')
    placed_on = placements(tenant['api'], vdev_id)
    item = {'id': vdev_id, 'nisds': placed_on, 'tenant': tenant}
    tenant['vdevs'].append(item)
    return item


def delete_one(item):
    tenant = item['tenant']
    tenant['api'].call('DELETE', '/api/vdev/' + item['id'])
    return item['id']


def collect_timing(api, observed):
    q = queue(api)
    for rec in q.get('recent_jobs') or []:
        if rec.get('job_type') == 1 or str(rec.get('job_type')) == '1':
            observed.add(str(rec.get('resource_id')))
    return q


def wait_jobs(args, items, servers):
    deadline = time.monotonic() + args.wait_seconds
    observed = defaultdict(set)
    last = {}
    while time.monotonic() < deadline:
        for i, server in enumerate(servers):
            try:
                q = collect_timing(server, observed[i])
                last[f'server{i}'] = {k: q.get(k) for k in ('pending', 'processing', 'failed')}
            except Exception as e:
                last[f'server{i}'] = str(e)
        pending = []
        for item in items:
            rows = jobs_for(args, item['tenant']['schema'], item['id'])
            require(len(rows) <= 1, f'Duplicate jobs for {item["id"]}: {rows}')
            if rows and int(rows[0][3]) == 3:
                raise AssertionError(f'Cleanup failed for {item["id"]}: {rows[0]}')
            if len(rows) != 1 or int(rows[0][3]) != 2:
                pending.append(item['id'])
        if not pending:
            # Extra sample to catch final recent timings.
            for i, server in enumerate(servers):
                collect_timing(server, observed[i])
            return observed
        time.sleep(args.poll_seconds)
    raise AssertionError(f'Timed out: {len(pending)} pending / missing jobs: {pending[:10]}; queues={last}')


def verify(args, tenants, items):
    for tenant in tenants:
        api = tenant['api']
        final_caps = resources(api)
        require(final_caps == tenant['capacity_before'],
                f'Capacity mismatch tenant={tenant["id"]}: before={tenant["capacity_before"]}, after={final_caps}')
        for item in tenant['vdevs']:
            rows = jobs_for(args, tenant['schema'], item['id'])
            require(len(rows) == 1, f'Expected one job for {item["id"]}, got {rows}')
            row = rows[0]
            require(int(row[1]) == 1 and int(row[3]) == 2 and row[4] == '' and row[5] == '1',
                    f'Invalid completed job {item["id"]}: {row}')
            found = events(api, item['id'])
            got = Counter(str(e.get('nisd_id')) for e in found)
            expected = Counter({n: 1 for n in item['nisds']})
            require(got == expected,
                    f'Unexpected cleanup events for {item["id"]}: expected={expected}, got={got}')
            require(all(int(e.get('action', -1)) == 1 for e in found),
                    f'Wrong cleanup event action for {item["id"]}: {found}')
            # GET /api/vdev?id returns application error on not-found in some builds.
            _, raw = api.call('GET', '/api/vdev', params={'id': item['id']}, allow_failure=True)
            payload = unwrap(raw)
            require(not (isinstance(payload, dict) and payload.get('id') == item['id']),
                    f'Deleted VDEV is still returned as active: {item["id"]}')
    print(f'PASS: {len(items)} cleanup jobs; no duplicate events; all tenant capacities restored')


def spawn_secondary(args, port, ordinal):
    binary = Path(args.server_binary).resolve()
    require(binary.is_file() and os.access(binary, os.X_OK),
            f'Executable server binary missing: {binary}')
    env = os.environ.copy()
    env['MDSVC_LISTEN'] = f':{port}'
    env['MDSVC_API_URL'] = f'http://127.0.0.1:{port}'
    env['MDSVC_MYSQL_HOST'] = args.mysql_host
    env['MDSVC_MYSQL_PORT'] = str(args.mysql_port)
    env['MDSVC_MYSQL_USER'] = args.mysql_user
    env['MDSVC_MYSQL_PASSWORD'] = args.mysql_password
    env['MDSVC_DSN'] = (f'{args.mysql_user}:{args.mysql_password}@tcp({args.mysql_host}:{args.mysql_port})/'
                        if args.mysql_password else f'{args.mysql_user}@tcp({args.mysql_host}:{args.mysql_port})/')
    log_path = Path(args.logs) / f'mdsvc-secondary-{ordinal}.log'
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open('w')
    proc = subprocess.Popen([str(binary)], cwd=str(binary.parent), env=env,
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return proc, log, log_path


def wait_ready(proc, base, seconds=45):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        require(proc.poll() is None, f'Secondary mdsvc-api exited rc={proc.returncode}')
        try:
            r = requests.get(base + '/swagger/', timeout=2)
            if r.status_code in (200, 301, 302):
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise AssertionError(f'Secondary server not ready at {base}')


def verify_worker_logs(paths, offsets):
    """Inspect only this test's log portion, not unrelated earlier runs."""
    found = []
    for path in paths:
        path = Path(path)
        if not path.exists():
            raise AssertionError(f'Cleanup server log missing: {path}')
        with path.open('rb') as fp:
            fp.seek(offsets.get(str(path), 0))
            text = fp.read().decode('utf-8', errors='replace')
        for line in text.splitlines():
            if '[cleanup]' in line.lower() and re.search(r'\b(error|failed|timeout|panic)\b', line, re.I):
                found.append(f'{path}: {line[:300]}')
    require(not found, 'Unexpected cleanup worker log errors: ' + '; '.join(found[:12]))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--mode', choices=['single', 'multi'], required=True)
    p.add_argument('--api', default='http://127.0.0.1:8081')
    p.add_argument('--username', default=os.environ.get('INTEGRATION_ADMIN_USERNAME', 'admin'))
    p.add_argument('--password', default=os.environ.get('INTEGRATION_ADMIN_PASSWORD', 'admin'))
    p.add_argument('--infra-json', required=True)
    p.add_argument('--vdev-json', required=True)
    p.add_argument('--tenants', type=int, default=3)
    p.add_argument('--vdevs-per-tenant', type=int, default=3)
    p.add_argument('--multi-count', type=int, default=100)
    p.add_argument('--mysql-host', default='127.0.0.1')
    p.add_argument('--mysql-port', type=int, default=4000)
    p.add_argument('--mysql-user', default='root')
    p.add_argument('--mysql-password', default=os.environ.get('MDSVC_MYSQL_PASSWORD', ''))
    p.add_argument('--server-binary')
    p.add_argument('--secondary-ports', nargs=2, type=int, default=[8082, 8083])
    p.add_argument('--logs', default='./cleanup-worker-logs')
    p.add_argument('--wait-seconds', type=int, default=600)
    p.add_argument('--poll-seconds', type=float, default=0.5)
    args = p.parse_args()
    require(args.tenants >= 2 if args.mode == 'single' else args.multi_count >= 2,
            'Insufficient jobs/tenants for selected scenario')
    require(args.mode != 'multi' or args.server_binary, '--server-binary required for multi mode')
    Path(args.logs).mkdir(parents=True, exist_ok=True)
    admin = API(args.api).login(args.username, args.password)
    # Fail early if the binary does not include test-only handlers.
    queue(admin)
    secondaries = []
    primary_log = Path(args.server_binary).resolve().parent / 'mdsvc.log' if args.server_binary else None
    log_offsets = {}
    if primary_log and primary_log.exists():
        log_offsets[str(primary_log)] = primary_log.stat().st_size
    try:
        if args.mode == 'multi':
            for ordinal, port in enumerate(args.secondary_ports, 2):
                proc, handle, logpath = spawn_secondary(args, port, ordinal)
                secondaries.append((proc, handle, logpath, port))
                wait_ready(proc, f'http://127.0.0.1:{port}')
        tenant_count = args.tenants if args.mode == 'single' else 1
        tenants = [create_tenant(admin, f'clm-{args.mode}-{uuid.uuid4().hex[:10]}')
                   for _ in range(tenant_count)]
        items = []
        for tenant in tenants:
            ensure_infra(tenant, args.infra_json)
            tenant['capacity_before'] = resources(tenant['api'])
            count = args.vdevs_per_tenant if args.mode == 'single' else args.multi_count
            for i in range(count):
                items.append(create_vdev(tenant, args.vdev_json, i))
        # Prevent false capacity assertion if VDEV allocation never changed anything.
        for tenant in tenants:
            after_create = resources(tenant['api'])
            require(after_create != tenant['capacity_before'],
                    f'VDEV creation did not change NISD capacity for {tenant["id"]}')
        # Login per server using the tenant credentials for server-local timing.
        servers = [tenants[0]['api']]
        for proc, handle, logpath, port in secondaries:
            servers.append(API(f'http://127.0.0.1:{port}').login(
                tenants[0]['username'], tenants[0]['password'], tenants[0]['id']))
        # Send deletion requests concurrently so the shared queue accumulates work.
        with ThreadPoolExecutor(max_workers=min(16, len(items))) as pool:
            futures = [pool.submit(delete_one, it) for it in items]
            for f in as_completed(futures):
                f.result()
        observed = wait_jobs(args, items, servers)
        verify(args, tenants, items)
        log_paths = [primary_log] if primary_log else []
        log_paths += [logpath for proc, handle, logpath, port in secondaries]
        for proc, handle, logpath, port in secondaries:
            handle.flush()
        if log_paths:
            verify_worker_logs(log_paths, log_offsets)
        if args.mode == 'multi':
            contributing = {i: len(ids & {it['id'] for it in items})
                            for i, ids in observed.items() if ids & {it['id'] for it in items}}
            require(len(contributing) >= 2,
                    f'Could not prove multiple servers processed jobs; recent timings={contributing}. '
                    'Increase backlog and enable debug logs if timing ring was overwritten.')
            print(f'PASS: multiple app servers contributed (observed timings): {contributing}')
        report = {'mode': args.mode, 'tenants': len(tenants), 'jobs': len(items),
                  'server_jobs_observed': {str(i): len(v) for i, v in observed.items()}}
        (Path(args.logs) / 'result.json').write_text(json.dumps(report, indent=2))
    finally:
        for proc, handle, path, port in secondaries:
            try:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    proc.wait(timeout=10)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGKILL)
            handle.close()
            print(f'Secondary server log: {path}')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)
