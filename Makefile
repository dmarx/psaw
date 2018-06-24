
define msg
    @printf "\033[36m# %s\033[0m\n" $(1)
endef

test:  ## Run tests
	$(call msg,"Running tests")
	py.test --show-capture=no psaw/

format: ## Run Black Python formatter
	$(call msg,"Running Black Python formatter")
	find psaw -iname "*.py" | xargs black

lint:  ## Run PyLint
	$(call msg,"Running PyLint")
	find psaw -iname "*.py" | xargs pylint
