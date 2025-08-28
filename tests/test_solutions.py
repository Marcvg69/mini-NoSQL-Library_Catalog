import mongomock
from src.solutions import (
    remove_empty_or_null_author_last_name, distinct_author_first_names,
    convert_date_fields, ten_oldest_books,
    count_by_country_python, count_by_country_pipeline
)

SEED = [
  {"title":"The Hobbit","author_first_name":"J.R.R.","author_last_name":"Tolkien","country":"UK","published_date":"1937-09-21","out_of_print_date":None},
  {"title":"The Little Prince","author_first_name":"Antoine","author_last_name":"de Saint-Exupéry","country":"France","published_date":"1943-04-06","out_of_print_date":None},
  {"title":"Things Fall Apart","author_first_name":"Chinua","author_last_name":"Achebe","country":"Nigeria","published_date":"1958-06-17","out_of_print_date":None},
  {"title":"One Hundred Years of Solitude","author_first_name":"Gabriel","author_last_name":"García Márquez","country":"Colombia","published_date":"1967-05-30","out_of_print_date":None},
  {"title":"The Trial","author_first_name":"Franz","author_last_name":"Kafka","country":"Czechia","published_date":"1925-04-26","out_of_print_date":None},
  {"title":"Untitled Draft","author_first_name":"Ada","author_last_name":"","country":"UK","published_date":"1815-12-10","out_of_print_date":"1852-11-27"},
  {"title":"Placeholder","author_first_name":"Test","author_last_name":None,"country":"Nowhere","published_date":"2000-01-01","out_of_print_date":None}
]

def make_coll():
    client = mongomock.MongoClient()
    col = client["library_db"]["books"]
    col.insert_many(SEED)
    return col

def test_remove_empty_or_null():
    col = make_coll()
    deleted = remove_empty_or_null_author_last_name(col)
    assert deleted == 2
    assert col.count_documents({"author_last_name": {"$in": ["", None]}}) == 0

def test_distinct_first_names():
    col = make_coll()
    remove_empty_or_null_author_last_name(col)
    names = distinct_author_first_names(col)
    assert "Chinua" in names and "Franz" in names
    assert "Ada" not in names  # deleted

def test_convert_and_oldest():
    col = make_coll()
    remove_empty_or_null_author_last_name(col)
    updated = convert_date_fields(col, ["published_date","out_of_print_date"])
    assert updated >= 1
    oldest = ten_oldest_books(col)
    assert len(oldest) >= 3
    assert oldest[0]["title"] in {"The Trial", "The Hobbit"}  # earliest dates

def test_counts():
    col = make_coll()
    remove_empty_or_null_author_last_name(col)
    py = count_by_country_python(col)
    agg = count_by_country_pipeline(col)
    agg_d = {r["_id"]: r["count"] for r in agg}
    for k,v in py.items():
        assert agg_d.get(k) == v
