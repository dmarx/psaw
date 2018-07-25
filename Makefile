PROJECT_NAME=pushshift.py
SRC_PATH=src
TESTS_PATH=tests

define msg
    @printf "\033[36m# %s\033[0m\n" $(1)
endef

test:
	$(call msg,"Running tests")
	python -m pytest $(TESTS_PATH)

format:
	$(call msg,"Running Black Python formatter")
	find $(SRC_PATH) $(TESTS_PATH) -iname "*.py" | xargs black

lint:
	$(call msg,"Running PyLint (minus TODOs)")
	find $(SRC_PATH) $(TESTS_PATH) -iname "*.py" | xargs pylint --disable=fixme

todos:
	$(call msg,"Retrieving TODO lines")
	find $(SRC_PATH) $(TESTS_PATH) -iname "*.py" | xargs pylint | grep '\[W0511(fixme),*'

code-coverage:
	$(call msg,"Running coverage.py")
	python -m pytest --cov=$(SRC_PATH) $(TESTS_PATH)

travis-coverage:
	$(call msg,"Running coverage.py formatted for build")
	python -m pytest --cov-report xml --cov=$(SRC_PATH) $(TESTS_PATH)

create-venv:
	$(call msg,"Creating a virtualenv for this project")
	virtualenv venv
	venv/bin/pip install -r requirements.txt
	echo "$(shell pwd)/$(SRC_PATH)" > venv/lib/python3.6/site-packages/$(PROJECT_NAME).pth
	echo "$(shell pwd)/$(TESTS_PATH)" > venv/lib/python3.6/site-packages/test-$(PROJECT_NAME).pth

publish:
	$(call msg,"Publishing to PyPI")
	rm dist/*.whl | true
	rm dist/*.tar.gz | true
	python setup.py sdist bdist_wheel
	twine upload dist/*
