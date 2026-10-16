#!/usr/bin/env python3
"""Destructive, dedicated-runner niova-ublk crash/recovery test.

A rollback-counter query is optional until its ctl-interface path is known.
Without a query this script reports RECOVERY_ONLY, never ROLLBACK_PASS.
"""
import argparse
import json
import os
import pathlib
import re
import shutil
import signal
import stat
import subprocess
import sys
import time

def run(argv, **kwargs):
    return subprocess.run(argv, check=True, text=True, **kwargs)

def devices():
    return {str(p) for p in pathlib.Path('/dev').glob('ublkb*') if stat.S_ISBLK(p.stat().st_mode)}

def wait_device(before, process, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if process.poll() is not None:
            raise RuntimeError(f'niova-ublk exited unexpectedly: rc={process.returncode}')
        new = devices() - before
        if len(new) == 1:
            return next(iter(new))
        if len(new) > 1:
            raise RuntimeError(f'ambiguous new ublk devices: {new}')
        time.sleep(.25)
    raise RuntimeError('new ublk block device did not appear')


def stop_process(proc, log):
    if proc is None or proc.poll() is not None:
        return
    log('sending SIGKILL to niova-ublk process group')
    os.killpg(proc.pid, signal.SIGKILL)
    try:
        proc.wait(timeout=12)
    except subprocess.TimeoutExpired:
        raise RuntimeError('niova-ublk did not exit after SIGKILL')


def wait_device_gone(device, timeout):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if not pathlib.Path(device).exists():
            return
        time.sleep(.25)
    raise RuntimeError(f'stale device after crash: {device}; clean up on dedicated test host before retrying')

def fio_write(device, blocksize, logpath, *, background=False, verify=False, name='write', offset='0', size='256M', runtime=90, bssplit=''):
    cmd = ['fio', f'--name={name}', f'--filename={device}', '--rw=randwrite' if background else '--rw=write',
           f'--bs={blocksize}', f'--offset={offset}', f'--size={size}', '--direct=1',
           '--ioengine=io_uring', '--iodepth=32', '--group_reporting=1', '--output-format=json',
           f'--output={logpath}']
    if bssplit:
        cmd += [f'--bssplit={bssplit}']
    if background:
        cmd += ['--time_based=1', f'--runtime={runtime}', '--randrepeat=0']
    if verify:
        cmd += ['--verify=crc32c', '--verify_fatal=1', '--do_verify=0']
    if background:
        return subprocess.Popen(cmd, start_new_session=True)
    run(cmd)
    result = json.loads(pathlib.Path(logpath).read_text())
    if any(j.get('error', 1) != 0 for j in result.get('jobs', [])) or not result.get('jobs'):
        raise RuntimeError(f'fio failed; inspect {logpath}')

def verify(device, blocksize, logpath, size='64M', offset='512M'):
    run(['fio', '--name=recovery-verify', f'--filename={device}', '--rw=read',
         f'--bs={blocksize}', f'--size={size}', f'--offset={offset}', '--direct=1',
         '--ioengine=io_uring', '--iodepth=16', '--verify=crc32c', '--verify_only=1',
         '--verify_fatal=1', '--output-format=json', f'--output={logpath}'])
    data = json.loads(pathlib.Path(logpath).read_text())
    if not data.get('jobs') or any(job.get('error', 1) != 0 for job in data['jobs']):
        raise RuntimeError('post-crash read/CRC verification failed')

def counter(command, regex, logdir, attempt):
    if not command:
        return None
    output = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=30)
    (logdir / f'counter-{attempt}.txt').write_text(output.stdout + '\nSTDERR:\n' + output.stderr)
    if output.returncode:
        raise RuntimeError(f'counter command failed: rc={output.returncode}')
    match = re.search(regex, output.stdout)
    if not match:
        raise RuntimeError('rollback counter missing from ctl-interface output')
    return int(match.group(1))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=['ece', 'ecre', 'mixed'], required=True)
    ap.add_argument('--vdev', required=True)
    ap.add_argument('--binary', required=True)
    ap.add_argument('--logdir', required=True)
    ap.add_argument('--retries', type=int, default=6)
    ap.add_argument('--timeout', type=int, default=30)
    ap.add_argument('--counter-command', default='')
    ap.add_argument('--counter-regex', default=r'rollback-cnt[^0-9]*([0-9]+)')
    ap.add_argument('--require-counter', action='store_true')
    args = ap.parse_args()
    if os.geteuid() != 0:
        ap.error('run as root (Ansible become: true); required for block device and SIGKILL')
    if args.retries < 1:
        ap.error('--retries must be at least 1')
    if devices():
        ap.error('dedicated runner required: existing /dev/ublkb* found; will not modify unrelated devices')
    for command in ('fio',):
        if not shutil.which(command):
            ap.error(f'{command} missing')
    binary = pathlib.Path(args.binary).resolve()
    if not binary.is_file():
        ap.error(f'niova-ublk executable missing: {binary}')
    logdir = pathlib.Path(args.logdir)
    logdir.mkdir(parents=True, exist_ok=True)
    def log(message):
        print(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] {message}', flush=True)
    # The foreground verification region is isolated from the crash workload region.
    sizes = {'ece': ['512k'], 'ecre': ['4k'], 'mixed': ['4k', '16k', '64k', '128k', '256k']}[args.mode]
    p = None
    writer = None
    output = None
    try:
        rolled_back = False
        for attempt in range(1, args.retries + 1):
            before = devices()
            output = open(logdir / f'client-{attempt}.log', 'a')
            p = subprocess.Popen([str(binary), '-t', 'cp', '-v', args.vdev, '-q', '128', '-b', '1048576', '-T'],
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            device = wait_device(before, p, args.timeout)
            log(f'attempt={attempt} started client pid={p.pid} device={device}')
            # Bounded background fio workload; kill while writer is still alive.
            bs = sizes[(attempt - 1) % len(sizes)]
            split = '4k/20:16k/20:64k/20:128k/20:256k/20' if args.mode == 'mixed' else ''
            writer = fio_write(device, bs, logdir / f'background-{attempt}.json', background=True,
                               name='crash-load', size='256M', offset='0', runtime=90, bssplit=split)
            time.sleep(2)
            if writer.poll() is not None:
                raise RuntimeError('background writer terminated before crash injection')
            stop_process(p, log)
            p = None
            try:
                writer.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(writer.pid, signal.SIGKILL)
                writer.wait(timeout=5)
            writer = None
            wait_device_gone(device, args.timeout)
            output.close()
            output = open(logdir / f'client-restart-{attempt}.log', 'a')
            before = devices()
            p = subprocess.Popen([str(binary), '-t', 'cp', '-v', args.vdev, '-q', '128', '-b', '1048576', '-T'],
                                 stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            recovered_device = wait_device(before, p, args.timeout)
            value = counter(args.counter_command, args.counter_regex, logdir, attempt)
            log(f'attempt={attempt} restarted client device={recovered_device} rollback-cnt={value}')
            if value is not None and value > 0:
                rolled_back = True
            # A second write in a disjoint area, followed by CRC verification.
            fio_write(recovered_device, '4k', logdir / f'post-write-{attempt}.json', verify=True,
                      name='recovery-write', size='64M', offset='512M')
            verify(recovered_device, '4k', logdir / f'post-verify-{attempt}.json')
            if rolled_back:
                log(f'ROLLBACK_PASS: {args.mode} attempt={attempt}, counter={value}; post-crash I/O verified')
                return 0
            if not args.counter_command:
                log(f'RECOVERY_ONLY: {args.mode} crash/restart and post-crash I/O verified; rollback-cnt NOT CHECKED')
                return 2 if args.require_counter else 0
            stop_process(p, log)
            p = None
            wait_device_gone(recovered_device, args.timeout)
            output.close()
            output = None
        raise RuntimeError(f'rollback-cnt did not exceed zero in {args.retries} attempts')
    finally:
        if writer and writer.poll() is None:
            os.killpg(writer.pid, signal.SIGKILL)
            writer.wait()
        if p is not None:
            stop_process(p, log)
        if output is not None:
            output.close()

if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'FAIL: {exc}', file=sys.stderr)
        sys.exit(1)