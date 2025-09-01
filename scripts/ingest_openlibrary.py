#!/usr/bin/env python3
import argparse, gzip, json, os, sys
from pathlib import Path
from pymongo import MongoClient, InsertOne
from tqdm import tqdm

# Reuse your date parsing to match notebook/tests behavior
try:
    from src.solutions import parse_date_maybe
except Exception:
    # fallback minimal
    from datetime import datetime
    def parse_date_maybe(v):
        if v is None: return None
        s = str(v).strip()
        if not s: return None
        for fmt in ("%Y-%m-%d","%Y/%m/%d","%d/%m/%Y","%m/%d/%Y","%Y.%m.%d","%d-%m-%Y","%Y"):
            try:
                if fmt == "%Y":
                    return datetime(int(s),1,1)
                return datetime.strptime(s, fmt)
            except: pass
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00")).replace(tzinfo=None)
        except: return None

def open_maybe_gzip(path: Path):
    if str(path).endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")

def parse_dump_line(line: str):
    # Lines are typically: "<key>\t<json>\t<timestamp>\n"
    # Sometimes there may be only json. We try robustly.
    parts = line.split("\t")
    if len(parts) >= 2:
        try:
            return json.loads(parts[1])
        except Exception:
            pass
    # Fallback: try whole line as JSON
    try:
        return json.loads(line)
    except Exception:
        return None

def split_name(full: str):
    # naive: last token as last_name; rest as first_name
    if not full: return (None, None)
    tokens = full.strip().split()
    if len(tokens) == 1:
        return (tokens[0], None)
    return (" ".join(tokens[:-1]), tokens[-1])

def load_authors_map(authors_path: Path, limit: int|None):
    m = {}
    count = 0
    with open_maybe_gzip(authors_path) as fh:
        for line in tqdm(fh, desc="scan authors"):
            obj = parse_dump_line(line)
            if not obj: continue
            key = obj.get("key")  # e.g. "/authors/OL123A"
            name = obj.get("name")
            if key and name:
                m[key] = split_name(name)
            count += 1
            if limit and count >= limit:
                break
    return m

def iter_works(works_path: Path, limit: int|None):
    count = 0
    with open_maybe_gzip(works_path) as fh:
        for line in tqdm(fh, desc="scan works"):
            obj = parse_dump_line(line)
            if not obj: continue
            yield obj
            count += 1
            if limit and count >= limit:
                return

def to_book_doc(work_obj: dict, authors_map: dict):
    title = work_obj.get("title")
    # first author if available
    akey = None
    for a in (work_obj.get("authors") or []):
        # can be {"author": {"key": "/authors/OL...A"}}
        author = a.get("author") if isinstance(a, dict) else None
        if isinstance(author, dict) and author.get("key"):
            akey = author["key"]
            break

    first, last = (None, None)
    if akey and akey in authors_map:
        first, last = authors_map[akey]

    # published_date: use first_publish_date if present
    pub = work_obj.get("first_publish_date")
    pub_dt = parse_date_maybe(pub)

    return {
        "title": title,
        "author_first_name": first,
        "author_last_name": last,
        "country": "Unknown",              # OL doesn't include reliable country per work
        "published_date": pub_dt,
        "out_of_print_date": None,
        "_source": "openlibrary_works",
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--authors", required=True, help="path to ol_dump_authors_latest.txt[.gz]")
    ap.add_argument("--works", required=True, help="path to ol_dump_works_latest.txt[.gz]")
    ap.add_argument("--mongo", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "library_db"))
    ap.add_argument("--coll", default=os.getenv("COLLECTION", "books"))
    ap.add_argument("--authors-scan-limit", type=int, default=500000, help="scan N author lines (speed vs coverage)")
    ap.add_argument("--works-limit", type=int, default=200000, help="ingest up to N works")
    ap.add_argument("--batch", type=int, default=1000)
    ap.add_argument("--drop", action="store_true", help="drop target collection before insert")
    args = ap.parse_args()

    authors_path = Path(args.authors).expanduser()
    works_path = Path(args.works).expanduser()
    client = MongoClient(args.mongo)
    col = client[args.db][args.coll]

    if args.drop:
        col.drop()

    print(f"Building authors map from: {authors_path}")
    authors_map = load_authors_map(authors_path, limit=args.authors_scan_limit)
    print(f"Authors in map: {len(authors_map):,}")

    ops, inserted = [], 0
    for w in iter_works(works_path, limit=args.works_limit):
        doc = to_book_doc(w, authors_map)
        if not doc.get("title"):
            continue
        ops.append(InsertOne(doc))
        if len(ops) >= args.batch:
            col.bulk_write(ops, ordered=False)
            inserted += len(ops)
            ops.clear()
    if ops:
        col.bulk_write(ops, ordered=False)
        inserted += len(ops)

    print(f"Inserted: {inserted:,} docs into {args.db}.{args.coll}")
    # Helpful indexes
    col.create_index("published_date")
    col.create_index("country")
    col.create_index([("author_last_name", 1), ("author_first_name", 1)])
    print("Indexes created.")

if __name__ == "__main__":
    sys.exit(main())
