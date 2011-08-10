#!/usr/bin/env python
import time
import subprocess


def cpu_metrics(start_time):
    # aggregate cpu stats
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-u', 'ALL'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    # cpuid is -1 for the aggregate
    hostname,interval,timestamp,cpuid,data = lines[-1].split(';',4)
    data = [float(x) for x in data.split(';')]
    for i, name in enumerate(('user', 'nice', 'sys', 'iowait', 'steal','irq', 'soft', 'guest', 'idle')):
        metrics[name] = {
            'ts': int(timestamp),
            'type': 'GAUGE',
            'value': data[i],
        }

    return metrics


def paging_metrics(start_time):
    # paging stats
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-B'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def load_metrics(start_time):
    # queue and load
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-q'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def mem_metrics(start_time):
    # memory utilization (these miss the active/inactive from vmstat -s though i'm not sure how cool those are)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-r'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def io_metrics(start_time):
    # i/o stuff
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-b'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def network_metrics(start_time):
    # this is already per second values so they're GAUGES (which might suck)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-n', 'DEV'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
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


def tables_metrics(start_time):
    # inode, file and other kernel tables
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-v'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def proc_context_metrics(start_time):
    # proc/s and context switches/s
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-w'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def swapping_metrics(start_time):
    # swapping
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-W'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')
    if not lines or lines[-1].startswith('#'): return metrics
    hostname,interval,timestamp,data = lines[-1].split(';',3)
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


def blockdev_metrics(start_time):
    # block devices (only reports back sda, mdX (mdadm) and dev251 (lvm?)
    metrics = {}

    p = subprocess.Popen(['/usr/bin/sadf', '-Dt', '-s', start_time, '--', '-dp'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    lines = stdout.strip('\r\n').split('\n')

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
    start_time = time.strftime("%H:%M:%S",time.gmtime(time.time()-300))
    callback('sar-cpu', cpu_metrics(start_time))
    callback('sar-io', io_metrics(start_time))
    callback('sar-load', load_metrics(start_time))
    callback('sar-mem', mem_metrics(start_time))
    callback('sar-paging', paging_metrics(start_time))
    callback('sar-swapping', swapping_metrics(start_time))


if __name__ ==  '__main__':
    import json
    print json.dumps(io_metrics(), indent=2)

