#!/usr/bin/env bash
set -euo pipefail

WRITERS=(1 2 4 8 16 32)
#WRITERS=(1 2)
STORES=("umadb" "kurrentdb" "axonserver" "eventsourcingdb")
WORKLOAD_TEMPLATE="workloads/concurrent_writers.yaml"
RAW_DIR="results/raw"
PUBLISHED_DIR="results/published"

cargo build --release -p esbs

# Clean previous results
rm -rf "$RAW_DIR" "$PUBLISHED_DIR"

for w in "${WRITERS[@]}"; do
  # Generate a per-count workload YAML on the fly
  WORKLOAD_FILE="workloads/concurrent_writers_w${w}.yaml"
  sed "s/^writers:.*/writers: ${w}/" "$WORKLOAD_TEMPLATE" \
    | sed "s/^name:.*/name: concurrent_writers_w${w}/" \
    > "$WORKLOAD_FILE"

  for store in "${STORES[@]}"; do
    echo "=== Running $store with $w writers ==="
    ./target/release/esbs run \
      --store "$store" \
      --workload "$WORKLOAD_FILE" \
      --output "$RAW_DIR"
  done

  # Clean up generated workload file
  rm -f "$WORKLOAD_FILE"
done

# Generate consolidated report
python python/report_generator.py --raw "$RAW_DIR" --out "$PUBLISHED_DIR"
echo "Done! Open $PUBLISHED_DIR/index.html"
