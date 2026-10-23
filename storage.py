# durable Raft state (Raft §5.1, Figure 2): the current term, voted_for and the log must
# survive a crash, and must reach disk before we answer the RPC that changed them.
# a snapshot (Raft §7) stands in for the start of the log once it has been applied

import json
import os


def _fsync_dir(path):
    # makes a rename or newly created file in this directory durable
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# write a new file and rename it over the old one, so a crash leaves either the old
# contents or the new ones, never half of each
def _replace_file(path, data):
    tmp = path + '.tmp'
    with open(tmp, 'wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(os.path.dirname(path))


# term and voted_for, saved together as one small file
class PersistentState:
    def __init__(self, path):
        self.path = path

        try:
            with open(path) as f:
                state = json.load(f)
            self.term = state['term']
            self.voted_for = tuple(
                state['voted_for']) if state['voted_for'] else None
        except FileNotFoundError:
            self.term, self.voted_for = 0, None

    def save(self, term, voted_for):
        _replace_file(self.path, json.dumps(
            {'term': term, 'voted_for': voted_for}).encode())
        self.term, self.voted_for = term, voted_for


# the state machine as of log index `index` (whose entry had term `term`). it replaces
# every log entry up to and including that index. -1 means no snapshot yet
class Snapshot:
    def __init__(self, path):
        self.path = path

        try:
            with open(path) as f:
                snapshot = json.load(f)
            self.index, self.term, self.data = (
                snapshot['index'], snapshot['term'], snapshot['data'])
        except FileNotFoundError:
            self.index, self.term, self.data = -1, 0, {}

    def save(self, index, term, data):
        _replace_file(self.path, json.dumps(
            {'index': index, 'term': term, 'data': data}).encode())
        self.index, self.term, self.data = index, term, data


def _header(index, term):
    return json.dumps({'snapshot_index': index, 'snapshot_term': term}).encode() + b'\n'


def _entry_line(entry):
    return json.dumps(list(entry)).encode() + b'\n'


# the log entries after the latest snapshot, as a file of JSON lines. the first line says
# where the log starts; every line after it is one (term, cmd) entry. Raft only changes a
# log by appending, dropping a conflicting suffix, or (with snapshots) dropping a prefix,
# so the file supports exactly those. reads come from an in-memory copy.
#
# indexes are absolute: entry i is the i-th entry ever logged, whether or not the entries
# before it have since been compacted into a snapshot
class PersistentLog:
    def __init__(self, path):
        self.path = path

        if not os.path.exists(path):
            _replace_file(path, _header(-1, 0))

        self.entries = []
        # byte offset in the file where each entry starts, so truncate() knows where to cut
        self.offsets = []

        with open(path, 'rb') as f:
            header = json.loads(f.readline())
            self.snapshot_index = header['snapshot_index']
            self.snapshot_term = header['snapshot_term']
            size = f.tell()

            for line in f:
                try:
                    term, cmd = json.loads(line)
                except ValueError:
                    # a crash mid-write leaves a partial last line. it was never fsynced,
                    # so nobody was told about it; drop it
                    break
                self.entries.append((term, cmd))
                self.offsets.append(size)
                size += len(line)

        self.f = open(path, 'ab')
        if self.f.tell() != size:
            self._sync_truncate(size)
        self.size = size

    # index of the last entry, counting ones compacted into the snapshot
    def last_index(self):
        return self.snapshot_index + len(self.entries)

    def last_term(self):
        return self.term_at(self.last_index())

    # term of entry i. works for the last snapshotted entry too, and for -1 (before the
    # first entry), so callers never need to special-case either
    def term_at(self, i):
        if i == self.snapshot_index:
            return self.snapshot_term
        return self[i][0]

    # the (term, cmd) entry at index i, which must come after the snapshot
    def __getitem__(self, i):
        if not self.snapshot_index < i <= self.last_index():
            raise IndexError(f"log index {i} is outside "
                             f"({self.snapshot_index}, {self.last_index()}]")
        return self.entries[i - self.snapshot_index - 1]

    def entries_from(self, i):
        return self.entries[i - self.snapshot_index - 1:]

    # iterates over the entries still in the log (those after the snapshot)
    def __iter__(self):
        return iter(self.entries)

    def __repr__(self):
        return f"<log after {self.snapshot_index}: {self.entries!r}>"

    def append(self, entry):
        self.extend([entry])

    def extend(self, entries):
        if not entries:
            return

        data = b''
        for entry in entries:
            self.offsets.append(self.size + len(data))
            data += _entry_line(entry)

        self.f.write(data)
        self.f.flush()
        os.fsync(self.f.fileno())

        self.entries.extend(tuple(e) for e in entries)
        self.size += len(data)

    # drop entry i and everything after it
    def truncate(self, i):
        if i > self.last_index():
            return

        offset = self.offsets[i - self.snapshot_index - 1]
        self._sync_truncate(offset)
        self.size = offset
        del self.entries[i - self.snapshot_index - 1:]
        del self.offsets[i - self.snapshot_index - 1:]

    # a snapshot now covers everything up to index (whose entry had term term). if our
    # entry there matches, keep what follows it; otherwise none of our log can be trusted
    # to match the snapshot, so drop it all (Raft §7)
    def compact(self, index, term):
        if index <= self.snapshot_index:
            return

        if index <= self.last_index() and self.term_at(index) == term:
            kept = self.entries_from(index + 1)
        else:
            kept = []

        self.f.close()
        _replace_file(self.path, _header(index, term) +
                      b''.join(_entry_line(e) for e in kept))
        self.f = open(self.path, 'ab')

        self.snapshot_index, self.snapshot_term = index, term
        self.entries = list(kept)

        size = len(_header(index, term))
        self.offsets = []
        for entry in kept:
            self.offsets.append(size)
            size += len(_entry_line(entry))
        self.size = size

    def _sync_truncate(self, size):
        self.f.flush()
        self.f.truncate(size)
        os.fsync(self.f.fileno())

    def close(self):
        self.f.close()


# where a server keeps its durable state; relative to the cwd, like config/
def data_dir(addr):
    path = os.path.join('data', str(addr[1]))
    os.makedirs(path, exist_ok=True)
    return path
