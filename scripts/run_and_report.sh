#!/usr/bin/env bash
set -euo pipefail

#WRITERS=(1 2 4 8 16)
WRITERS=(4 8)
#STORES=("umadb" "kurrentdb" "axonserver" "eventsourcingdb")
STORES=("umadb" "kurrentdb")
WORKFLOWS=("concurrent_writers")
WORKLOAD_TEMPLATE="workloads/concurrent_writers.yaml"
RAW_DIR="results/raw"
PUBLISHED_DIR="results/published"

cargo build --release -p esbs

# Clean previous results
rm -rf "$RAW_DIR" "$PUBLISHED_DIR"

for workflow in "${WORKFLOWS[@]}"; do
  for w in "${WRITERS[@]}"; do
    # Generate a per-count workload YAML on the fly
    WORKLOAD_FILE="workloads/${workflow}_w${w}.yaml"
    sed "s/^writers:.*/writers: ${w}/" "$WORKLOAD_TEMPLATE" \
      | sed "s/^name:.*/name: ${workflow}_w${w}/" \
      > "$WORKLOAD_FILE"

    for store in "${STORES[@]}"; do
      echo "=== Running $store with workflow $workflow and $w writers ==="
      ./target/release/esbs run \
        --store "$store" \
        --workflow "$workflow" \
        --workload "$WORKLOAD_FILE" \
        --output "$RAW_DIR"
    done

    # Clean up generated workload file
    rm -f "$WORKLOAD_FILE"
  done
done

# Generate consolidated report
python python/report_generator.py --raw "$RAW_DIR" --out "$PUBLISHED_DIR"
echo "Done! Open $PUBLISHED_DIR/index.html"
