PYTHON_VERSION=3.8.6

setup:
	make python
	make venv
	make install

python:
	pyenv install ${PYTHON_VERSION} --skip-existing
	pyenv local ${PYTHON_VERSION}

venv:
	python3 -m venv .venv

activate:
	. ./.venv/bin/activate

reqs: 
	pip install pip==20.3.3
	pip install -r requirements.txt

install:
	make activate && make reqs