ALLSPHINXOPTS= -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
MODULE=wal_e
VIRTUALENV=$(shell echo "$${VDIR:-'.env'}")
PYTHON=$(VIRTUALENV)/bin/python

all: $(VIRTUALENV)

$(VIRTUALENV): requirements.txt
	@virtualenv --no-site-packages $(VIRTUALENV)
	@$(VIRTUALENV)/bin/pip install -r requirements.txt
	$(PYTHON) setup.py develop
	touch $(VIRTUALENV)

.PHONY: help
# target: help - Display callable targets
help:
	@egrep "^# target:" [Mm]akefile | sed -e 's/^# target: //g'

.PHONY: clean
# target: clean - Clean repo
clean:
	@rm -rf build dist docs/_build
	@rm -f *.py[co]
	@rm -f *.orig
	@rm -f */*.py[co]
	@rm -f */*.orig

.PHONY: register
# target: register - Register module on PyPi
register:
	$(PYTHON) setup.py register

.PHONY: upload
# target: upload - Upload module on PyPi
upload: clean
	$(PYTHON) setup.py sdist upload || echo 'Upload already'

.PHONY: test
# target: test - Run module tests
test: clean
	PATH=$(VIRTUALENV)/bin:$(PATH) $(PYTHON) setup.py test
