.PHONY: test

test:
	python3 -m unittest discover -s tests -p "*_test.py"
