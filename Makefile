.PHONY: up down seed run fmt lint test

up:
\tdocker compose up -d

down:
\tdocker compose down -v

seed:
\tpython scripts/import_data.py data/sample_books.json

run:
\tpython scripts/run_all.py

fmt:
\tblack src scripts tests

lint:
\tflake8 src scripts tests

test:
\tpytest -q
