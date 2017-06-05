TEST_FILES = 2

install:
	pip3 install -r requirements.txt

test:
	@# use '-v' for higher verbosity
	python3 -m unittest discover -s ./spec -p "*_test.py"
