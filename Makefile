MODULE=wal_e
SPHINXBUILD=sphinx-build
ALLSPHINXOPTS= -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
BUILDDIR=_build
PYTHON=.env/bin/python

all: .env

.PHONY: help
# target: help - Display callable targets
help:
	@egrep "^# target:" [Mm]akefile

.PHONY: clean
# target: clean - Clean repo
clean:
	sudo rm -rf build dist
	find . -name "*.pyc" -delete
	find . -name "*.orig" -delete

.PHONY: register
# target: register - Register module on PyPi
register:
	python setup.py register

.PHONY: upload
# target: upload - Upload module on PyPi
upload: clean
	python setup.py sdist upload || echo 'Upload already'

.PHONY: test
# target: test - Run module tests
test: .env
	$(PYTHON) setup.py test

.env: requirements.txt
	virtualenv --no-site-packages .env
	.env/bin/pip install -M -r requirements.txt
