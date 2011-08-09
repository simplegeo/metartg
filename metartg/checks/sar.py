#!/usr/bin/env python
from time import time
import subprocess


def cpu_metrics():
    # aggregate cpu stats
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-u', 'ALL', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        # cpuid is -1 for the aggregate
        hostname,interval,timestamp,cpuid,data = line.split(';',4)
        data = [float(x) for x in data.split(';')]
        for i, name in enumerate(('user', 'nice', 'sys', 'iowait', 'steal','irq', 'soft', 'guest', 'idle')):
            metrics[name] = {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': data[i],
            }

    return metrics


def paging_metrics():
    # paging stats
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-B', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        kbpgin_s,kbpgout_s,pgfault_s,majfault_s,pgfree_s,pgscank_s,pgscand_s,pgsteal_s,extras = data.split(';')
        metrics.update({
            'kb_pagein_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(kbpgin_s),
            },
            'kb_pageout_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(kbpgout_s),
            },
            'page_fault_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pgfault_s),
            },
            'major_fault_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(majfault_s),
            },
            'pages_freed_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pgfree_s),
            },
            'pages_scanned_kswapd_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pgscank_s),
            },
            'pages_scanned_directly_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pgscand_s),
            },
            'pages_stolen_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pgsteal_s),
            },
        })

    return metrics


def load_metrics():
    # queue and load
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-q', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        runqsz,plistsz,ldavg1,ldavg5,ldavg15 = data.split(';')
        metrics.update({
            'run_queue_length': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(runqsz),
            },
            'proc_list_size': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(plistsz),
            },
            'load_avg_1m': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(ldavg1),
            },
            'load_avg_5m': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(ldavg5),
            },
            'load_avg_15m': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(ldavg15),
            },
        })

    return metrics


def mem_metrics():
    # memory utilization (these miss the active/inactive from vmstat -s though i'm not sure how cool those are)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-r', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        kbmemfree,kbmemused,pct_memused,kbbuffers,kbcached,kbcommit,pct_commit = data.split(';')
        metrics.update({
            'free_memory': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(kbmemfree),
            },
            'used_memory': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(kbmemused),
            },
            'buffer_memory': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(kbbuffers),
            },
            'swap_cache': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(kbcached),
            },
            'total_memory': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(int(kbmemfree)+int(kbmemused)),
            },
        })

    return metrics


def io_metrics():
    # i/o stuff
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-b', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        tps,rtps,wtps,bread_s,bwritten_s = data.split(';')
        metrics.update({
            'io_req_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(tps),
            },
            'read_io_req_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(rtps),
            },
            'write_io_req_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(wtps),
            },
            'bytes_read_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(bread_s),
            },
            'bytes_written_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(bwritten_s),
            },
        })

    return metrics


def network_metrics():
    # this is already per second values so they're GAUGES (which might suck)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-n', 'DEV', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,iface,rxpps,txpps,rxkBps,txkBps,rxcmpps,txcmppps,rxmcpps = line.split(';')
        if not iface.startswith('eth'): continue
        metrics.update({
            '%s_rx_bytes' % iface: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(rxkBps),
            },
            '%s_tx_bytes' % iface: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(txkBps),
            },
            '%s_rx_packets' % iface: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(rxpps),
            },
            '%s_tx_packets' % iface: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(txpps),
            }
        })

    return metrics


def tables_metrics():
    # inode, file and other kernel tables
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-v', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        dentunusd,file_nr,inode_nr,pty_nr = data.split(';')
        metrics.update({
            'used_dircache_entries': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(dentunusd),
            },
            'used_file_handles': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(file_nr),
            },
            'used_inode_handlers': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(inode_nr),
            },
            'used_ptys': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': int(pty_nr),
            },
        })

    return metrics


def proc_context_metrics():
    # proc/s and context switches/s
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-w', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        procs_s,cswch_s = data.split(';')
        metrics.update({
            'procs_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(procs_s),
            },
            'context_switches_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(cswch_s),
            },
        })

    return metrics


def swapping_metrics():
    # swapping
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-W', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,data = line.split(';',3)
        pswpin_s,pswpout_s = data.split(';')
        metrics.update({
            'pages_swapped_in_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pswpin_s),
            },
            'pages_swapped_out_sec': {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(pswpout_s),
            },
        })

    return metrics


def blockdev_metrics():
    # block devices (only reports back sda, mdX (mdadm) and dev251 (lvm?)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '--', '-dp', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    for line in stdout.strip('\r\n').split('\n'):
        if line.startswith('#'): continue
        hostname,interval,timestamp,device,data = line.split(';',4)
        if not device.startswith('sda') and not device.startswith('md') and not device.startswith('dev251'): continue
        tps,rd_sec_s,wr_sec_s,avgrq_sz,avgqu_sz,await,svctm,pct_util = data.split(';')
        metrics.update({
            '%s_io_req_sec' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(tps),
            },
            '%s_sectors_read_sec' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(rd_sec_s),
            },
            '%s_sectors_written_sec' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(wr_sec_s),
            },
            '%s_avg_request_size' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(avgrq_sz),
            },
            '%s_avg_queue_length' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(avgqu_sz),
            },
            '%s_avg_time' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(await),
            },
            '%s_avg_service_time' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(svctm),
            },
            '%s_pct_cpu_during_io' % device: {
                'ts': int(timestamp),
                'type': 'GAUGE',
                'value': float(await),
            },
        })

    return metrics


def run_check(callback):
    callback('sar-cpu', cpu_metrics())
    callback('sar-io', io_metrics())
    callback('sar-load', load_metrics())


if __name__ ==  '__main__':
    import json
    print json.dumps(io_metrics(), indent=2)

