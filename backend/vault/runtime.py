# Lightweight in-process performance counters for the hackathon dashboard.
# Metadata/data durability lives in SQLite and node storage; these counters are telemetry only.

active_writes = 0
active_reads = 0
max_concurrent_writes = 0
max_concurrent_reads = 0
total_writes = 0
total_reads = 0


def write_started():
    global active_writes, max_concurrent_writes, total_writes
    active_writes += 1
    total_writes += 1
    max_concurrent_writes = max(max_concurrent_writes, active_writes)


def write_finished():
    global active_writes
    active_writes = max(0, active_writes - 1)


def read_started():
    global active_reads, max_concurrent_reads, total_reads
    active_reads += 1
    total_reads += 1
    max_concurrent_reads = max(max_concurrent_reads, active_reads)


def read_finished():
    global active_reads
    active_reads = max(0, active_reads - 1)


def snapshot():
    return {
        "active_writes": active_writes,
        "active_reads": active_reads,
        "max_concurrent_writes": max_concurrent_writes,
        "max_concurrent_reads": max_concurrent_reads,
        "total_writes": total_writes,
        "total_reads": total_reads,
    }
