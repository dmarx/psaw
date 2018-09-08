# Get version number from file
# dynamically define filename of wheel object from version number
# Make a build target for the current version's wheel
# add as a dependency to the publish make targets

.PHONY: wheel test-publish publish

wheel:
	python setup.py sdist bdist_wheel

test-publish: wheel
	twine upload --repository-url https://test.pypi.org/legacy/ dist/$(ls dist | tail -1)

publish: wheel
	twine upload dist/$(ls dist | tail -1)
