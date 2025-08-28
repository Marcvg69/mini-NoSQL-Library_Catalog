# src/solutions.py

from datetime import datetime
from collections import Counter

__all__ = [
    "remove_empty_or_null_author_last_name",
    "unique_author_first_names",
    "distinct_author_first_names",   # alias expected by tests
    "convert_date_fields",
    "oldest_by_published_date",
    "ten_oldest_books",              # alias expected by tests
    "count_by_country_python",
    "count_by_country_pipeline",
]

# ---------------- helpers ----------------

def parse_date_maybe(v):
    """Parse many common date formats to a naive datetime; return None if not parseable."""
    if v is None or isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    fmts = (
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
        "%Y/%m/%d", "%Y.%m.%d", "%d-%m-%Y", "%Y"
    )
    for fmt in fmts:
        try:
            if fmt == "%Y":
                return datetime(int(s), 1, 1)
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # ISO 8601 fallback
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

# ---------------- tasks ----------------

def remove_empty_or_null_author_last_name(col):
    """Delete docs where author_last_name is empty string or null. Return deleted count."""
    flt = {"$or": [{"author_last_name": ""}, {"author_last_name": None}]}
    res = col.delete_many(flt)
    return res.deleted_count


def unique_author_first_names(col):
    """Sorted (case-insensitive) unique, non-empty author_first_name values."""
    vals = col.distinct("author_first_name")
    return sorted([v for v in vals if v not in (None, "")], key=str.lower)


def distinct_author_first_names(col):
    """Alias kept for compatibility with tests."""
    return unique_author_first_names(col)


def convert_date_fields(col, fields):
    """
    Convert string date fields to BSON datetimes.
    Implemented with per-document update (mongomock-safe; no bulk_write).
    Returns number of modified documents.
    """
    updated = 0
    for f in fields:
        cursor = col.find({f: {"$type": "string"}}, {"_id": 1, f: 1})
        for d in cursor:
            new_val = parse_date_maybe(d.get(f))
            res = col.update_one({"_id": d["_id"]}, {"$set": {f: new_val}})
            updated += res.modified_count
    return updated


def oldest_by_published_date(col, limit=10):
    """
    Return the oldest documents by published_date (ascending).
    Projects common fields used by the exercises/tests.
    """
    cur = (
        col.find(
            {"published_date": {"$type": "date"}},
            {
                "_id": 0,
                "title": 1,
                "author_first_name": 1,
                "author_last_name": 1,
                "country": 1,
                "published_date": 1,
            },
        )
        .sort("published_date", 1)
        .limit(limit)
    )
    return list(cur)


def ten_oldest_books(col):
    """Alias required by tests: returns 10 oldest books by published_date."""
    return oldest_by_published_date(col, limit=10)


def count_by_country_python(col):
    """
    Count docs by country in Python. Missing/None -> 'Unknown'.
    Returns a dict like {'USA': 3, 'Unknown': 1, ...}.
    """
    cnt = Counter((d.get("country") or "Unknown") for d in col.find({}, {"country": 1}))
    # Ensure plain ints (already are, but explicit for safety)
    return {k: int(v) for k, v in cnt.items()}


def count_by_country_pipeline(col):
    """
    Aggregation pipeline version.
    Returns a list of dicts like {'_id': <country or 'Unknown'>, 'count': <int>},
    sorted by count desc then country asc — exactly what the tests expect.
    """
    pipe = [
        {"$group": {"_id": {"$ifNull": ["$country", "Unknown"]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
    ]
    res = list(col.aggregate(pipe))
    for d in res:
        d["count"] = int(d["count"])
    return res
