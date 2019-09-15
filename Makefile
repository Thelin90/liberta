include src/.env

.PHONY: clean
clean:
	-find . -type f -name "*.pyc" -delete
	-find . -type f -name "*,cover" -delete

.PHONY: test
test:
	PYTHONPATH=. pytest . -v

.PHONY: tablestopg
tablestopg:
	PYTHONPATH=. spark-submit $(PYSPARK_JARS_ARGS) --packages $(PYSPARK_SUBMIT_ARGS) --conf $(PYSPARK_CONF_ARGS) src/run_tables_to_pg.py