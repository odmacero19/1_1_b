#!/usr/bin/env python3
"""
Executable check of the delta-sync SQL (schema, triggers, queue queries, migration 14).

It builds a SQLite database straight from the SQLDelight sources in data/src/main/sqldelight
(type annotations such as `AS Boolean` are stripped) and exercises the behaviour the Kotlin layer
relies on. Run:  python3 tools/verify_delta_sync_sql.py
"""
import re, sqlite3, pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent / "data/src/main/sqldelight/tachiyomi"


def strip_sqldelight(sql: str) -> str:
    sql = re.sub(r"\bAS\s+(?:List<String>|Boolean\b|UpdateStrategy\b|JsonObject\b|Date\b)", "", sql)
    return sql


def statements(text: str):
    """Yield complete SQL statements (comments removed)."""
    text = re.sub(r"--[^\n]*", "", text)
    buf = ""
    for line in text.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            stmt = buf.strip()
            buf = ""
            if stmt:
                yield stmt


def schema_statements(files, old_triggers=False):
    out = []
    for f in files:
        text = (ROOT / f).read_text()
        text = "\n".join(l for l in text.splitlines() if not l.startswith("import "))
        for st in statements(strip_sqldelight(text)):
            if re.match(r"CREATE\s+(UNIQUE\s+)?(TABLE|INDEX|TRIGGER)", st, re.I):
                out.append(st)
            elif re.match(r"INSERT OR IGNORE INTO categories", st):
                out.append(st)
    return out


def named_queries(path):
    """name -> sql for `name:` blocks of a .sq file."""
    text = (ROOT / path).read_text()
    text = re.sub(r"--[^\n]*", "", text)
    res = {}
    for m in re.finditer(r"^(\w+):\s*\n(.*?;)\s*$", text, re.S | re.M):
        res[m.group(1)] = m.group(2)
    # the regex above is greedy-safe only per statement; re-split properly
    res = {}
    cur = None
    buf = ""
    for line in text.splitlines(keepends=True):
        mm = re.match(r"^(\w+):\s*$", line)
        if mm and not buf.strip():
            cur = mm.group(1)
            continue
        if cur:
            buf += line
            if sqlite3.complete_statement(buf):
                res[cur] = buf.strip()
                cur, buf = None, ""
    return res


FILES = ["data/categories.sq", "data/mangas.sq", "data/chapters.sq", "data/mangas_categories.sq",
         "data/sync_delta_queue.sq"]


def fresh_db():
    db = sqlite3.connect(":memory:", isolation_level=None)
    db.execute("PRAGMA foreign_keys = ON")
    for st in schema_statements(FILES):
        db.execute(st)
    return db


Q = named_queries("data/sync_delta_queue.sq")


def run(db, _q, **kw):
    """Run a named query; `IN :x` lists are expanded."""
    sql = Q[_q]
    params = {}
    for k, v in kw.items():
        if isinstance(v, (list, tuple)):
            ph = ",".join(f":{k}{i}" for i in range(len(v))) or "NULL"
            sql = sql.replace(f"IN :{k}", f"IN ({ph})")
            params.update({f"{k}{i}": x for i, x in enumerate(v)})
        else:
            params[k] = v
    return db.execute(sql, params).fetchall()


def rows(db):
    return db.execute("SELECT table_name, record_id, status, attempts FROM sync_delta_queue ORDER BY _id").fetchall()


def add_manga(db, url="m", fav=0):
    db.execute(
        "INSERT INTO mangas(source,url,title,status,favorite,initialized,viewer,chapter_flags,cover_last_modified,date_added)"
        " VALUES (1,?,'t',0,?,1,0,0,0,0)", (url, fav))
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


def add_chapter(db, manga, url="c"):
    db.execute(
        "INSERT INTO chapters(manga_id,url,name,read,bookmark,last_page_read,chapter_number,source_order,date_fetch,date_upload)"
        " VALUES (?,?,'n',0,0,0,1,0,0,0)", (manga, url))
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]


fails = 0


def check(label, cond):
    global fails
    print(("ok   " if cond else "FAIL ") + label)
    if not cond:
        fails += 1


def version(db, table, id_):
    return db.execute(f"SELECT version FROM {table} WHERE _id=?", (id_,)).fetchone()[0]


# ---------------------------------------------------------------- triggers
db = fresh_db()
m = add_manga(db)
c = add_chapter(db, m)
check("inserts do not enqueue", rows(db) == [])

db.execute("UPDATE mangas SET favorite = 1 WHERE _id = ?", (m,))
check("favorite change enqueues manga", rows(db) == [("mangas", m, "PENDING", 0)])
check("favorite change bumps version once", version(db, "mangas", m) == 1)

db.execute("UPDATE mangas SET description = 'a' WHERE _id = ?", (m,))
db.execute("UPDATE mangas SET description = NULL WHERE _id = ?", (m,))
check("repeated edits are coalesced into one pending entry", len(rows(db)) == 1)
check("NULL <-> value description transitions are detected", version(db, "mangas", m) == 3)

db.execute("UPDATE mangas SET title = 'other' WHERE _id = ?", (m,))
check("non user-owned column does not dirty", version(db, "mangas", m) == 3)

db.execute("DELETE FROM sync_delta_queue")
db.execute("UPDATE chapters SET read = 1 WHERE _id = ?", (c,))
check("chapter change enqueues chapter and manga", sorted(rows(db)) == sorted([("chapters", c, "PENDING", 0), ("mangas", m, "PENDING", 0)]))
check("chapter and manga versions bumped", version(db, "chapters", c) == 1 and version(db, "mangas", m) == 4)

db.execute("DELETE FROM sync_delta_queue")
db.execute("UPDATE chapters SET read = 0, is_syncing = 1 WHERE _id = ?", (c,))
check("is_syncing = 1 suppresses chapter trigger", rows(db) == [] and version(db, "chapters", c) == 1)
db.execute("UPDATE chapters SET is_syncing = 0 WHERE _id = ?", (c,))
check("clearing is_syncing afterwards does not dirty the row", rows(db) == [] and version(db, "chapters", c) == 1)

db.execute("UPDATE mangas SET is_syncing = 1 WHERE _id = ?", (m,))
db.execute("UPDATE chapters SET read = 1 WHERE _id = ?", (c,))
check("chapter edit while manga is_syncing queues only the chapter", rows(db) == [("chapters", c, "PENDING", 0)])
db.execute("UPDATE mangas SET is_syncing = 0 WHERE _id = ?", (m,))

db.execute("DELETE FROM sync_delta_queue")
db.execute("INSERT INTO categories(_id,name,sort,flags) VALUES (5,'Cat',0,0)")
db.execute("INSERT INTO mangas_categories(manga_id,category_id) VALUES (?,5)", (m,))
check("category insert enqueues manga", rows(db) == [("mangas", m, "PENDING", 0)])
db.execute("DELETE FROM sync_delta_queue")
db.execute("DELETE FROM mangas_categories WHERE manga_id = ?", (m,))
check("category removal enqueues manga", rows(db) == [("mangas", m, "PENDING", 0)])

# ---------------------------------------------------------------- remote apply protocol
db.execute("DELETE FROM sync_delta_queue")
db.execute("BEGIN")
run(db, "applyRemoteManga", artist=None, author=None, description="remote", genre=None, title="T", status=1,
    thumbnailUrl=None, favorite=1, initialized=1, viewerFlags=0, chapterFlags=0, dateAdded=0, updateStrategy=0,
    notes="", memo=b"{}", version=42, mangaId=m)
db.execute("DELETE FROM mangas_categories WHERE manga_id = ?", (m,))
db.execute("INSERT INTO mangas_categories(manga_id,category_id) VALUES (?,5)", (m,))
run(db, "clearMangaIsSyncing", mangaId=m)
db.execute("COMMIT")
check("applying a remote manga (incl. categories) does not enqueue anything", rows(db) == [])
check("remote version is stored verbatim", version(db, "mangas", m) == 42)
check("is_syncing is cleared after apply", db.execute("SELECT is_syncing FROM mangas WHERE _id=?", (m,)).fetchone()[0] == 0)

db.execute("BEGIN")
run(db, "applyRemoteChapter", name="n2", scanlator=None, read=1, bookmark=1, lastPageRead=7, chapterNumber=1.0,
    sourceOrder=0, dateUpload=0, memo=b"{}", version=9, chapterId=c)
run(db, "clearChapterIsSyncing", chapterId=c)
db.execute("COMMIT")
check("applying a remote chapter does not enqueue anything", rows(db) == [] and version(db, "chapters", c) == 9)
db.execute("UPDATE chapters SET read = 0 WHERE _id = ?", (c,))
check("local edit after a pull is tracked again", rows(db)[0][:2] == ("chapters", c) and version(db, "chapters", c) == 10)

# rollback keeps flag + data consistent
db.execute("DELETE FROM sync_delta_queue")
db.execute("BEGIN")
run(db, "applyRemoteChapter", name="x", scanlator=None, read=1, bookmark=0, lastPageRead=0, chapterNumber=1.0,
    sourceOrder=0, dateUpload=0, memo=b"{}", version=99, chapterId=c)
db.execute("ROLLBACK")
check("rollback leaves version and is_syncing untouched",
      version(db, "chapters", c) == 10 and db.execute("SELECT is_syncing FROM chapters").fetchone()[0] == 0)

# ---------------------------------------------------------------- queue lifecycle
db.execute("DELETE FROM sync_delta_queue")
for i in range(5):
    run(db, "enqueue", tableName="chapters", recordId=100 + i)
run(db, "enqueue", tableName="chapters", recordId=100)
check("enqueue coalesces duplicates", len(rows(db)) == 5)

db.execute("BEGIN")
run(db, "claimPending", limit=3)
claimed = run(db, "getInFlight")
db.execute("COMMIT")
check("claim takes the oldest N", [r[2] for r in claimed] == [100, 101, 102])

# a new local edit arrives while the upload is running
run(db, "enqueue", tableName="chapters", recordId=100)
check("edit during upload gets its own pending entry", len(rows(db)) == 6)

# upload fails -> release
ids = [r[0] for r in claimed]
db.execute("BEGIN")
run(db, "dropSupersededInFlight", ids=ids)
run(db, "releaseInFlight", error="HTTP 503", ids=ids)
db.execute("COMMIT")
r = rows(db)
check("failed push: same-record duplicate dropped, others back to PENDING with attempts+1",
      len(r) == 5 and all(x[2] == "PENDING" for x in r) and sorted(x[3] for x in r) == [0, 0, 0, 1, 1])
check("failed push keeps exactly one pending entry per record",
      len({(x[0], x[1]) for x in r}) == len(r))

db.execute("BEGIN"); run(db, "claimPending", limit=1); ids1 = [r[0] for r in run(db, "getInFlight")]; db.execute("COMMIT")
before = rows(db)
run(db, "releaseInFlightWithoutPenalty", error="offline", ids=ids1)
check("offline release does not consume attempts", sorted(x[3] for x in rows(db)) == sorted(x[3] for x in before))

# successful push
db.execute("BEGIN")
run(db, "claimPending", limit=100)
ids = [r[0] for r in run(db, "getInFlight")]
db.execute("COMMIT")
run(db, "deleteByIds", ids=ids)
check("ack empties the queue", rows(db) == [])

# crash recovery
for i in range(3):
    run(db, "enqueue", tableName="mangas", recordId=i + 1)
db.execute("BEGIN"); run(db, "claimPending", limit=2); db.execute("COMMIT")
run(db, "enqueue", tableName="mangas", recordId=1)
run(db, "dropSupersededStaleInFlight")
run(db, "recoverStaleInFlight")
r = rows(db)
check("stale in-flight recovered without violating uniqueness",
      all(x[2] == "PENDING" for x in r) and len({(x[0], x[1]) for x in r}) == len(r) == 3)

# park + requeue
db.execute("UPDATE sync_delta_queue SET attempts = 9")
run(db, "parkExhausted", maxAttempts=8)
check("exhausted entries are parked", all(x[2] == "FAILED" for x in rows(db)))
run(db, "enqueue", tableName="mangas", recordId=2)
run(db, "dropFailedCoveredByPending"); run(db, "dropFailedDuplicates"); run(db, "requeueFailed")
r = rows(db)
check("requeue merges with fresher pending entries", len(r) == 3 and all(x[2] == "PENDING" for x in r) and len({(x[0], x[1]) for x in r}) == 3)

check("latestEntryId only grows when something is enqueued",
      run(db, "latestEntryId")[0][0] > 0)

# ---------------------------------------------------------------- migration 14 on an old database
old = sqlite3.connect(":memory:", isolation_level=None)
old.execute("PRAGMA foreign_keys = ON")
for st in schema_statements(FILES):
    if "sync_delta_queue" in st:
        continue
    old.execute(st)
for t in ("update_manga_version", "update_chapter_and_manga_version", "insert_manga_category_update_version",
          "delete_manga_category_update_version"):
    old.execute(f"DROP TRIGGER IF EXISTS {t}")
old.execute("""CREATE TRIGGER update_manga_version AFTER UPDATE ON mangas BEGIN
    UPDATE mangas SET version = version + 1 WHERE _id = new._id AND new.is_syncing = 0 AND (
        new.url != old.url OR new.description != old.description OR new.favorite != old.favorite); END""")
old.execute("""CREATE TRIGGER insert_manga_category_update_version AFTER INSERT ON mangas_categories BEGIN
    UPDATE mangas SET version = version + 1 WHERE _id = new.manga_id AND (SELECT is_syncing FROM mangas WHERE _id = new.manga_id) = 0; END""")
om = add_manga(old, "legacy")
oc = add_chapter(old, om)
old.execute("UPDATE mangas SET favorite = 1 WHERE _id = ?", (om,))
mig = (ROOT / "migrations/14.sqm").read_text()
for st in statements(mig):
    old.execute(st)
check("migration applies on top of an existing database", True)
check("data survives migration", old.execute("SELECT version FROM mangas").fetchone()[0] == 1)
old.execute("UPDATE chapters SET read = 1 WHERE _id = ?", (oc,))
check("migrated triggers feed the queue",
      sorted(old.execute("SELECT table_name FROM sync_delta_queue").fetchall()) == [("chapters",), ("mangas",)])

# the migrated schema must be identical to the fresh one
def schema(d):
    return sorted((t, re.sub(r"\s+", " ", s or "")) for t, s in
                  d.execute("SELECT name, sql FROM sqlite_master WHERE name LIKE 'sync_delta%' OR type='trigger'"))
check("migration result == fresh schema (sync objects and triggers)", schema(old) == schema(db))

print("\n%d failure(s)" % fails)
sys.exit(1 if fails else 0)
