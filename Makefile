.PHONY: build venv run report help

# Default target
help:
	@echo "Available targets:"
	@echo "  build         - Build the es-bench executable"
	@echo "  venv          - Create a Python virtual environment and install dependencies"
	@echo "  run <config>  - Run the benchmark with a specified config (e.g., 'make run smoke-test')"
	@echo "  report        - Run the Python report generator"

# Build the es-bench binary
build:
	cargo build --release

# Create Python virtual environment and install dependencies
venv:
	python3 -m venv ./.venv
	./.venv/bin/pip install -r ./python/requirements.txt

# Run the benchmark
# Use: make run smoke-test
run:
	@if [ -z "$(filter-out run,$(MAKECMDGOALS))" ]; then \
		echo "Error: Please specify a config file name. Example: make run smoke-test"; \
		exit 1; \
	fi
	./target/release/es-bench run --config ./configs/$(filter-out run,$(MAKECMDGOALS)).yaml --seed 42 --data-dir=./container-data

# Prevent make from interpreting arguments to 'run' as targets
%:
	@:

# Generate report from raw results
report:
	./.venv/bin/python3 python/report_generator.py --raw results/raw --out results/published
