venv:
	python3 -m venv .venv
	. ./.venv/bin/activate && make reqs

reqs: 
	pip install -r requirements.txt