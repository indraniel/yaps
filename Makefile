.PHONY: clean

init:
	pip install -r requirements.txt

test:
	nosetests tests

clean:
	find ./yaps -name "*.pyc" -exec rm {} \;
