PACKAGE_PATH=psaw

define msg
    @printf "\033[36m# %s\033[0m\n" $(1)
endef

test:  ## Run tests
	$(call msg,"Running tests")
	py.test $(PACKAGE_PATH)/

format: ## Run Black Python formatter
	$(call msg,"Running Black Python formatter")
	find $(PACKAGE_PATH) -iname "*.py" | xargs black

lint:  ## Run PyLint
	$(call msg,"Running PyLint (minus TODOs)")
	find $(PACKAGE_PATH) -iname "*.py" | xargs pylint --disable=fixme

todos: ## Run PyLint to get a list of TODOs
	$(call msg,"Retrieving TODO lines")
	find $(PACKAGE_PATH) -iname "*.py" | xargs pylint | grep '\[W0511(fixme), \]'

code-coverage: ## Run coverage.py
	$(call msg,"Running coverage.py")
	py.test --cov=$(PACKAGE_PATH) $(PACKAGE_PATH)/

travis-coverage: ## Run coverage.py formatted for build
	$(call msg,"Running coverage.py formatted for build")
	py.test --cov-report xml --cov=$(PACKAGE_PATH) $(PACKAGE_PATH)/ && cat coverage.xml

create-venv: ## Create a virtualenv for this project
	$(call msg,"Creating a virtualenv for this project")
	virtualenv venv
	venv/bin/pip install -r requirements.txt
	echo "$(shell pwd)/$(PACKAGE_PATH)" > venv/lib/python3.6/site-packages/$(PROJECT_NAME).pth

publish: ## Publish the project to PyPI
	$(call msg,"Publishing to PyPI")
	rm dist/*.whl | true
	rm dist/*.tar.gz | true
	python setup.py sdist bdist_wheel
	twine upload dist/*

