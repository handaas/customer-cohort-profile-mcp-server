"""SQLite 快照叠加层。发布后不可写；读游标不向上游取数。"""
from __future__ import annotations

import base64
import hashlib
import heapq
import hmac
import json
import os
import sqlite3
import stat
from contextlib import contextmanager

from server.common import Fault, Settings, later, now, pack, uid

CORE_DDL = '''
CREATE TABLE IF NOT EXISTS snapshots(
 id TEXT PRIMARY KEY, tenant TEXT NOT NULL, roster_id TEXT NOT NULL,
 parent_id TEXT REFERENCES snapshots(id), sealed INTEGER NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS snapshots_tenant ON snapshots(tenant,roster_id);
CREATE TABLE IF NOT EXISTS records(
 snapshot_id TEXT NOT NULL REFERENCES snapshots(id), view TEXT NOT NULL, item_key TEXT NOT NULL,
 body TEXT, PRIMARY KEY(snapshot_id,view,item_key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS store_meta(key TEXT PRIMARY KEY,value BLOB NOT NULL);
INSERT OR IGNORE INTO store_meta VALUES ('cursor_key',randomblob(32));
'''
ANCESTORS = '''WITH RECURSIVE lineage(id,depth) AS (
 VALUES(?,0) UNION ALL SELECT s.parent_id,l.depth+1 FROM snapshots s JOIN lineage l ON s.id=l.id WHERE s.parent_id IS NOT NULL) '''
VIEWS = frozenset(('identities', 'candidates', 'subjects', 'evidence', 'coverage'))


class Store:
    def __init__(self, settings: Settings):
        from server import execution, planning
        self.settings = settings
        path = settings.db_path
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            s = os.fstat(fd)
            if not stat.S_ISREG(s.st_mode) or s.st_mode & 0o077:
                raise Fault('configuration_error', '状态数据库必须是权限 0600 的私有文件。')
        finally:
            os.close(fd)
        with self.tx() as db:
            db.commit()
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript(CORE_DDL + '\n' + planning.DDL + '\n' + execution.DDL)
            self._cursor_key = db.execute("SELECT value FROM store_meta WHERE key='cursor_key'").fetchone()[0]

    @contextmanager
    def tx(self, write=False):
        db = sqlite3.connect(str(self.settings.db_path), isolation_level=None, timeout=15.0)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def new_snapshot(self, db, tenant, roster_id, parent_id=None, sealed=False):
        expires = later(self.settings.snapshot_ttl)
        if parent_id:
            parent = self.require_snapshot(db, tenant, parent_id)
            if parent['roster_id'] != roster_id:
                raise Fault('out_of_scope', '父快照不属于该名单。')
            expires = min(expires, parent['expires_at'])
        sid = uid('snap')
        db.execute('INSERT INTO snapshots VALUES(?,?,?,?,?,?,?)',
                   (sid, tenant, roster_id, parent_id, int(sealed), expires, now()))
        return sid

    @staticmethod
    def require_snapshot(db, tenant, snapshot_id):
        row = db.execute('SELECT * FROM snapshots WHERE id=? AND tenant=? AND sealed=1', (snapshot_id, tenant)).fetchone()
        if row is None:
            raise Fault('out_of_scope', '找不到可访问的已发布快照。')
        if row['expires_at'] <= now():
            raise Fault('snapshot_expired', '快照已过期；不能自动向上游重取。')
        return row

    @staticmethod
    def seal(db, snapshot_id):
        db.execute('UPDATE snapshots SET sealed=1 WHERE id=?', (snapshot_id,))

    @staticmethod
    def _set(db, snapshot_id, view, key, body):
        result = db.execute('''INSERT INTO records(snapshot_id,view,item_key,body)
            SELECT id,?,?,? FROM snapshots WHERE id=? AND sealed=0
            ON CONFLICT(snapshot_id,view,item_key) DO UPDATE SET body=excluded.body''', (view,key,body,snapshot_id))
        if not result.rowcount:
            raise Fault('out_of_scope', '不能写入未获准或已发布的快照。')

    def put(self, db, snapshot_id, view, item_key, record):
        self._set(db, snapshot_id, view, item_key, pack(record))

    def drop(self, db, snapshot_id, view, item_key):
        self._set(db, snapshot_id, view, item_key, None)

    @staticmethod
    def record(db, snapshot_id, view, item_key):
        row = db.execute(ANCESTORS + '''SELECT r.body FROM lineage l JOIN records r ON r.snapshot_id=l.id
            WHERE r.view=? AND r.item_key=? ORDER BY l.depth LIMIT 1''', (snapshot_id, view, item_key)).fetchone()
        return json.loads(row[0]) if row and row[0] is not None else None

    @staticmethod
    def records(db, snapshot_id, view, after='', limit=None):
        # Each snapshot contributes one sorted index scan from the cursor key.
        # Merge only until this page is full; do not sort the whole evidence set.
        layers = db.execute(ANCESTORS + 'SELECT id,depth FROM lineage', (snapshot_id,))
        sql = '''SELECT item_key,?,body FROM records
                 WHERE snapshot_id=? AND view=? AND item_key>? ORDER BY item_key'''
        streams = [db.execute(sql, (depth, layer, view, after)) for layer, depth in layers]
        previous = None
        for key, _, body in heapq.merge(*streams, key=lambda row: (row[0], row[1])):
            if key == previous:
                continue
            previous = key
            if body is None:  # A tombstone on the nearest layer hides the parent.
                continue
            yield key, json.loads(body)
            if limit is not None:
                limit -= 1
                if limit <= 0:
                    break

    def _mac(self, tenant, sid, view, key):
        message = pack([tenant, sid, view]).encode() + b'\0' + key
        return hmac.new(self._cursor_key, message, hashlib.sha256).digest()[:20]

    def get_page(self, tenant, args):
        sid, view, limit = args['snapshot_id'], args['view'], args.get('limit', 50)
        if view not in VIEWS or not 1 <= limit <= 200:
            raise Fault('out_of_scope', '不支持的视图或分页大小。')
        after = ''
        if 'cursor' in args:
            try:
                token = args['cursor']
                if not token.startswith('cur_'):
                    raise ValueError
                body = token[4:]
                buf = base64.b64decode(body + '=' * (-len(body) % 4), altchars=b'-_', validate=True)
                key = buf[20:]
                if not key or not hmac.compare_digest(buf[:20], self._mac(tenant, sid, view, key)):
                    raise ValueError
                after = key.decode('utf-8')
            except (ValueError, UnicodeError):
                raise Fault('out_of_scope', '游标不属于此租户、快照或视图。') from None
        with self.tx() as db:
            self.require_snapshot(db, tenant, sid)
            rows = list(self.records(db, sid, view, after, limit + 1))
        more = len(rows) > limit
        if more:
            rows.pop()
        page = {'snapshot_id': sid, 'view': view, 'limit': limit, 'returned': len(rows),
                'has_more': more, 'records': [r for _, r in rows]}
        if more:
            key = rows[-1][0].encode()
            signed_key = self._mac(tenant, sid, view, key) + key
            page['next_cursor'] = 'cur_' + base64.urlsafe_b64encode(signed_key).decode().rstrip('=')
        return page
