#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ingest Open Library "authors" and "works" dumps into MongoDB — plus a simple JSON demo.
Now with AUTO-DETECT for Open Library dump formats:
- JSON Lines (each line is a JSON object), or
- TSV where the LAST COLUMN is JSON (common OL dump format).

DEMO 1 — Open Library dumps (.gz):
----------------------------------
export MONGO_URI="mongodb://127.0.0.1:27017"
python scripts/ingest_openlibrary_dump.py \
  --authors data/openlibrary/ol_dump_authors_2025-07-31.txt.gz \
  --works   data/openlibrary/ol_dump_works_2025-07-31.txt.gz \
  --drop \
  --authors-scan-limit 300000 \
  --works-limit 50000 \
  --batch 1000 \
  --verify-after

DEMO 2 — Simple JSON sample (data/sample_books.json):
-----------------------------------------------------
# sample_books.json may be a JSON array OR JSON Lines; plain or gzipped (.gz)

export MONGO_URI="mongodb://127.0.0.1:27017"
python scripts/ingest_openlibrary_dump.py \
  --json-sample data/sample_books.json \
  --drop --db library_db --collection books \
  --batch 1000 \
  --verify-after

Verification (works for either demo):
-------------------------------------
mongosh --quiet "mongodb://127.0.0.1:27017" --eval '
const d=db.getSiblingDB("library_db");
print("count =", d.books.countDocuments());
printjson(d.books.findOne({}, {title:1, authors:1, subjects:1, _id:0}));
'
"""

import argparse
import gzip
import json
import os
import sys
from typing import Dict, Iterator, Optional, List, Any, Tuple

from pymongo import MongoClient, TEXT
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError
from tqdm import tqdm


# -------- logging --------
def log(msg: str):
    print(msg, flush=True)


# -------- IO helpers --------
def open_maybe_gzip(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


def iter_lines(path: str) -> Iterator[str]:
    with open_maybe_gzip(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            yield line


# -------- Format detection & parsing --------
def _try_parse_json(s: str) -> Optional[dict]:
    try:
        return json.loads(s)
    except Exception:
        return None


def detect_record_parser(path: str, sample: int = 50) -> Tuple[str, int, int]:
    """
    Returns (mode, json_hits, tsv_hits) where mode in {"json", "tsv_json"}.
    """
    json_hits = 0
    tsv_hits = 0
    with open_maybe_gzip(path) as f:
        for _ in range(sample):
            line = f.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue

            if _try_parse_json(line) is not None:
                json_hits += 1
                continue

            # TSV fallback: try parsing last column as JSON
            if "\t" in line:
                parts = line.split("\t")
                rec = _try_parse_json(parts[-1])
                if rec is not None:
                    tsv_hits += 1

    # prefer the mode with more hits; default to tsv if any tsv hits exist
    if tsv_hits > 0 and tsv_hits >= json_hits:
        return "tsv_json", json_hits, tsv_hits
    return "json", json_hits, tsv_hits


def iter_records(path: str) -> Iterator[dict]:
    """
    Yields parsed JSON records from either:
      - JSON Lines (whole line is JSON), or
      - TSV where LAST column is JSON.
    """
    mode, jh, th = detect_record_parser(path)
    log(f"[detect] {os.path.basename(path)} → mode={mode} (json_hits={jh}, tsv_hits={th})")
    for line in iter_lines(path):
        if mode == "json":
            rec = _try_parse_json(line)
            if rec is not None:
                yield rec
                continue
            # if a JSON line parse fails, fall back just in case it was TSV
            if "\t" in line:
                parts = line.split("\t")
                rec = _try_parse_json(parts[-1])
                if rec is not None:
                    yield rec
        else:
            # TSV-with-JSON as last column
            if "\t" in line:
                parts = line.split("\t")
                rec = _try_parse_json(parts[-1])
                if rec is not None:
                    yield rec
                    continue
            # rare: a pure JSON line sneaks in
            rec = _try_parse_json(line)
            if rec is not None:
                yield rec


# -------- Type handling for Open Library --------
def _normalize_type(value) -> Optional[str]:
    """
    Return canonical type string without leading '/type/'.
    Accepts strings or dicts {'key': '/type/work'}.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("key")
    if isinstance(value, str):
        if value.startswith("/type/"):
            value = value[len("/type/"):]
        return value
    return None


def _is_type(doc: dict, expected: str) -> bool:
    """
    Accept '/type/author' or dict {'key': '/type/author'} AND
    fall back to key prefixes '/authors/' and '/works/'.
    """
    t = _normalize_type(doc.get("type"))
    if t == expected:
        return True
    k = doc.get("key") or ""
    if isinstance(k, str):
        if expected == "author" and k.startswith("/authors/"):
            return True
        if expected == "work" and k.startswith("/works/"):
            return True
    return False


# -------- Open Library ingestion --------
def build_authors_map(path: str, scan_limit: Optional[int] = None) -> Dict[str, str]:
    log(f"[authors] Building authors map from: {path}")
    authors: Dict[str, str] = {}
    for i, rec in enumerate(tqdm(iter_records(path), desc="scan authors", unit="it")):
        if scan_limit and i >= scan_limit:
            break

        if not isinstance(rec, dict):
            continue
        if not _is_type(rec, "author"):
            continue

        key = rec.get("key")
        if not key:
            continue

        # Prefer 'name', fall back to 'personal_name'
        name = rec.get("name") or rec.get("personal_name")
        if not name:
            continue

        authors[key] = name

    log(f"[authors] Authors in map: {len(authors)}")
    if len(authors) == 0:
        log("[authors][WARN] authors_map is empty. No works will be joinable to names.")
    return authors


def _insert_batch(collection, batch: List[dict]) -> int:
    try:
        res = collection.insert_many(batch, ordered=False)
        return len(res.inserted_ids)
    except BulkWriteError as e:
        details = getattr(e, "details", None) or {}
        n = int(details.get("nInserted") or 0)
        return n


def ingest_works(
    path: str,
    authors_map: Dict[str, str],
    limit: Optional[int],
    batch_size: int,
    collection,
) -> int:
    log(f"[works] Ingesting from: {path}")
    inserted = 0
    batch: List[dict] = []
    seen = 0
    kept = 0
    for i, rec in enumerate(tqdm(iter_records(path), desc="scan works", unit="it")):
        if limit and i >= limit:
            break

        if not isinstance(rec, dict):
            continue
        if not _is_type(rec, "work"):
            continue

        seen += 1

        # Resolve author names from map
        names = []
        for a in rec.get("authors", []) or []:
            k = (a.get("author") or {}).get("key")
            if k and k in authors_map:
                names.append(authors_map[k])

        if not names:
            continue

        doc = {
            "key": rec.get("key"),
            "title": rec.get("title"),
            "authors": names,
            "subjects": rec.get("subjects") or [],
        }
        batch.append(doc)
        kept += 1

        if len(batch) >= batch_size:
            inserted += _insert_batch(collection, batch)
            batch = []

    if batch:
        inserted += _insert_batch(collection, batch)

    log(f"[works] seen={seen}, eligible={kept}, inserted={inserted}")
    return inserted


# -------- Simple JSON sample ingestion --------
def ingest_json_books(path: str, batch_size: int, collection) -> int:
    """
    Accepts:
      - JSON array file with book dicts
      - JSON Lines (.jsonl) file
      - Either can be gzipped (.gz)
    Expected keys: title (str), authors (list[str] or str), subjects (list[str] or str, optional)
    """
    log(f"[json] Ingesting from: {path}")

    if not os.path.exists(path):
        log(f"[json][ERROR] File not found: {path}")
        return 0
    try:
        sz = os.path.getsize(path)
    except OSError:
        sz = -1
    log(f"[json] size={sz} bytes")

    # Detect array vs JSONL by peeking first non-space char
    with open_maybe_gzip(path) as f:
        first = f.read(1)
        while first and first.isspace():
            first = f.read(1)

    inserted = 0
    if first == "[":
        log("[json] Detected JSON array")
        with open_maybe_gzip(path) as f:
            try:
                data = json.load(f)
            except Exception as e:
                log(f"[json][ERROR] Failed to parse JSON array: {e}")
                return 0
        batch: List[dict] = []
        kept = 0
        for rec in data:
            doc = _coerce_book_doc(rec)
            if not doc:
                continue
            batch.append(doc)
            kept += 1
            if len(batch) >= batch_size:
                inserted += _insert_batch(collection, batch)
                batch = []
        if batch:
            inserted += _insert_batch(collection, batch)
        log(f"[json] kept={kept}, inserted={inserted}")
    else:
        log("[json] Detected JSON Lines")
        batch: List[dict] = []
        kept = 0
        for line in iter_lines(path):
            rec = _try_parse_json(line)
            if rec is None:
                continue
            doc = _coerce_book_doc(rec)
            if not doc:
                continue
            batch.append(doc)
            kept += 1
            if len(batch) >= batch_size:
                inserted += _insert_batch(collection, batch)
                batch = []
        if batch:
            inserted += _insert_batch(collection, batch)
        log(f"[json] kept={kept}, inserted={inserted}")
    return inserted


def _coerce_book_doc(rec: Any) -> Optional[dict]:
    if not isinstance(rec, dict):
        return None
    title = rec.get("title")
    authors = rec.get("authors") or rec.get("author") or []
    if isinstance(authors, str):
        authors = [authors]
    if not title or not isinstance(authors, list) or not authors:
        return None
    subjects = rec.get("subjects") or []
    if isinstance(subjects, str):
        subjects = [subjects]
    return {"title": title, "authors": authors, "subjects": subjects}


# -------- Indexes --------
def ensure_indexes(col):
    existing = {tuple(sorted(ix["key"].items())) for ix in col.list_indexes()}
    want_text = (("title", "text"),)
    want_auth = (("authors", 1),)
    if want_text not in existing:
        col.create_index([("title", TEXT)])
    if want_auth not in existing:
        col.create_index([("authors", 1)])


# -------- Main --------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest Open Library dumps or a simple JSON sample into MongoDB"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--json-sample", help="Path to JSON/JSONL(.gz) with simple book docs")
    group.add_argument("--authors", help="Path to Open Library authors dump (.txt or .gz)")

    parser.add_argument("--works", help="Path to Open Library works dump (.txt or .gz)")
    parser.add_argument("--authors-scan-limit", type=int, default=None, help="Max author lines to scan")
    parser.add_argument("--works-limit", type=int, default=None, help="Max works to ingest")
    parser.add_argument("--batch", type=int, default=1000, help="Insert batch size")
    parser.add_argument("--drop", action="store_true", help="Drop target collection first")
    parser.add_argument("--verify-after", action="store_true", help="Print count/sample after ingest")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--db", default="library_db")
    parser.add_argument("--collection", default="books")
    args = parser.parse_args()

    # Connect + ping
    log(f"[db] Connecting to {args.mongo_uri}")
    try:
        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except ServerSelectionTimeoutError as e:
        log(f"[db][ERROR] Cannot connect to MongoDB at {args.mongo_uri}: {e}")
        return 2

    db = client[args.db]
    col = db[args.collection]
    log(f"[db] Target: {args.db}.{args.collection}")

    # Pre-count
    pre_count = col.estimated_document_count()
    log(f"[db] Pre-count: {pre_count}")

    # Drop if requested
    if args.drop:
        try:
            col.drop()
            log(f"[db] Dropped existing collection {args.db}.{args.collection}")
        except Exception as e:
            log(f"[db][WARN] Drop failed/ignored: {e}")

    total_inserted = 0

    if args.json_sample:
        log(f"[path] json-sample: {args.json_sample}")
        total_inserted = ingest_json_books(args.json_sample, args.batch, col)
    else:
        if not args.works:
            log("Error: --works is required when using --authors")
            return 2
        # Path checks
        for p in (args.authors, args.works):
            if not os.path.exists(p):
                log(f"[path][ERROR] Not found: {p}")
                return 2
            try:
                sz = os.path.getsize(p)
            except OSError:
                sz = -1
            log(f"[path] {p} size={sz} bytes")

        authors_map = build_authors_map(args.authors, scan_limit=args.authors_scan_limit)
        total_inserted = ingest_works(
            path=args.works,
            authors_map=authors_map,
            limit=args.works_limit,
            batch_size=args.batch,
            collection=col,
        )

    log(f"[done] Inserted: {total_inserted} docs into {args.db}.{args.collection}")
    ensure_indexes(col)
    log("[done] Indexes created.")

    # Verify
    if args.verify_after:
        post_count = col.count_documents({})
        log(f"[verify] Post-count: {post_count}")
        try:
            sample = col.find_one({}, {"_id": 0, "title": 1, "authors": 1, "subjects": 1})
            log(f"[verify] Sample: {json.dumps(sample, ensure_ascii=False)}")
        except Exception as e:
            log(f"[verify][WARN] Could not fetch sample: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
