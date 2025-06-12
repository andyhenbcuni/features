setup:
	pip3 install --upgrade pip && \
	pip3 install --upgrade uv
	uv sync --dev

test.e2e:
	pytest tests/algo_features/test_e2e.py -n 3
