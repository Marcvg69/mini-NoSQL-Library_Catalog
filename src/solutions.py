from datetime import datetime
from typing import List, Dict, Any, Iterable
from pymongo.collection import Collection
from pymongo import UpdateOne

def remove_empty_or_null_author_last_name(col: Collection) -> int:
    res = col.delete_many({"$or": [{"author_last_name": ""}, {"author_last_name": None}]})
    return res.deleted_count

def distinct_author_first_names(col: Collection) -> List[str]:
    vals = [v for v in col.distinct("author_first_name") if v not in (None, "")]
    return sorted(set(vals), key=str.lower)

def _parse_date_maybe(s):
    if s is None or isinstance(s, datetime): return s
    s = str(s).strip()
    if not s: return None
    fmts = ["%Y-%m-%d","%d/%m/%Y","%m/%d/%Y","%Y/%m/%d","%Y.%m.%d","%d-%m-%Y","%Y"]
    for fmt in fmts:
        try:
            if fmt == "%Y": return datetime(int(s),1,1)
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def convert_date_fields(col: Collection, fields: Iterable[str]) -> int:
    ops, updated = [], 0
    for f in fields:
        for doc in col.find({f: {"$type": "string"}}, {"_id": 1, f: 1}):
            parsed = _parse_date_maybe(doc.get(f))
            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": {f: parsed}}))
            if len(ops) >= 500:
                updated += col.bulk_write(ops, ordered=False).modified_count
                ops.clear()
    if ops:
        updated += col.bulk_write(ops, ordered=False).modified_count
    return updated

def ten_oldest_books(col: Collection) -> List[Dict[str, Any]]:
    cur = (col.find({"published_date": {"$type": "date"}},
                    {"_id": 0, "title": 1, "author_first_name": 1, "author_last_name": 1, "country": 1, "published_date": 1})
             .sort("published_date", 1).limit(10))
    return list(cur)

def count_by_country_python(col: Collection) -> Dict[str, int]:
    counts = {}
    for d in col.find({}, {"country": 1}):
        key = d.get("country") or "Unknown"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: (-x[1], x[0])))

def count_by_country_pipeline(col: Collection):
    pipe = [
        {"$group": {"_id": {"$ifNull": ["$country", "Unknown"]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1, "_id": 1}},
    ]
    return list(col.aggregate(pipe))
