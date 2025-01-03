.PHONY: test

test:
	python3 -m unittest discover -s tests -p "*_test.py"

airflow:
	export AIRFLOW_HOME=$(shell pwd) && \
	export AIRFLOW__CORE__LOAD_EXAMPLES=False && \
	export PYTHONPATH=$(shell pwd) && \
	airflow standalone
