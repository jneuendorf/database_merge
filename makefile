MODULE = database-merge
MAIN = ./main.py

install:
	pip3 install -r requirements.txt

test:
	@# remove files in case a test has been interrupted so `tearDown` has not happened
	rm -f test*.db
	@# use '-v' for higher verbosity
	python3 -m unittest discover --start-directory ./spec --pattern "*_test.py"

test-cov: test
	coverage run --source=. --omit=$(MAIN) -m unittest discover --start-directory ./spec --pattern "*_test.py"
	coverage report -m

test-cov-html: test-cov
	coverage html

run:
	python3 $(MAIN) --settings-file=./testsettings.yml --log=DEBUG

run_optimized:
	python3 -O $(MAIN) --settings-file=./testsettings.yml

lint:
	pylint $(MODULE)
	@#mypy --hide-error-context --show-column-numbers --follow-imports skip --disallow-untyped-calls --allow-untyped-defs --check-untyped-defs --disallow-subclassing-any --no-warn-incomplete-stub --warn-redundant-casts --warn-no-return --warn-return-any --warn-unused-ignores --ignore-missing-imports --strict-optional $(MAIN)
