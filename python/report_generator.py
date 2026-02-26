import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")


def load_runs(raw_dir: Path):
    runs = []
    for run_path in sorted(raw_dir.iterdir()):
        if not run_path.is_dir():
            continue
        summary_file = run_path / "summary.json"
        samples_file = run_path / "samples.jsonl"
        if summary_file.exists() and samples_file.exists():
            with open(summary_file) as f:
                summary = json.load(f)
            samples = []
            with open(samples_file) as f:
                for line in f:
                    samples.append(json.loads(line))
            runs.append({
                "path": run_path,
                "summary": summary,
                "samples": samples,
            })
    return runs


def plot_latency_cdf(samples: pd.DataFrame, out_path: Path):
    # Convert microseconds to milliseconds
    lat_ms = samples.loc[samples["ok"], "latency_us"].astype(float) / 1000.0
    lat_ms = lat_ms.clip(lower=1e-3)
    lat_sorted = np.sort(lat_ms.values)
    p = np.linspace(0, 100, len(lat_sorted), endpoint=False)
    plt.figure(figsize=(6,4))
    plt.plot(lat_sorted, p, label="append latency CDF")
    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title("Latency CDF")
    plt.grid(True, which="both", ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput(samples: pd.DataFrame, out_path: Path):
    # Bucket by 100ms and count OK appends per bucket
    t0 = samples["t_ms"].min()
    df = samples.copy()
    df["t_rel_ms"] = df["t_ms"] - t0
    df["bucket"] = (df["t_rel_ms"] // 100).astype(int)
    grp = df.groupby("bucket")["ok"].apply(lambda x: int(x.sum()))
    # convert to events/sec per 100ms bucket
    eps = grp * 10
    plt.figure(figsize=(6,4))
    plt.plot(eps.index.values / 10.0, eps.values)
    plt.xlabel("Time (s)")
    plt.ylabel("Events/sec")
    plt.title("Throughput over time")
    plt.grid(True, ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def generate_html(report_dir: Path, run):
    summary = run["summary"]
    latency_img = report_dir / "latency_cdf.png"
    throughput_img = report_dir / "throughput.png"

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>ESBS Report — {summary['adapter']} / {summary['workload']}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2 {{ margin-top: 1.2rem; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>ESBS Report</h1>
  <p><b>Adapter:</b> {summary['adapter']} &nbsp; | &nbsp; <b>Workload:</b> {summary['workload']}</p>
  <p><b>Duration:</b> {summary['duration_s']:.1f}s &nbsp; | &nbsp; <b>Throughput:</b> {summary['throughput_eps']:.0f} eps</p>
  <div class='row'>
    <div class='card'>
      <h2>Latency CDF</h2>
      <img src='{latency_img.name}' width='560'>
    </div>
    <div class='card'>
      <h2>Throughput over time</h2>
      <img src='{throughput_img.name}' width='560'>
    </div>
  </div>
</body>
</html>
"""
    with open(report_dir / "index.html", "w") as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser(description="Generate ESBS benchmark report from raw results")
    parser.add_argument("--raw", type=str, default="results/raw", help="Path to raw results dir")
    parser.add_argument("--out", type=str, default="results/published", help="Output reports dir")
    args = parser.parse_args()

    raw_dir = Path(args.raw)
    out_base = Path(args.out)
    out_base.mkdir(parents=True, exist_ok=True)

    runs = load_runs(raw_dir)
    if not runs:
        print(f"No runs found in {raw_dir}")
        return

    # Use the most recent run for now
    run = runs[-1]
    samples_df = pd.DataFrame(run["samples"])

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    report_dir = out_base / f"report-{run['summary']['adapter']}-{Path(run['summary']['workload']).stem}-{ts}"
    report_dir.mkdir(parents=True, exist_ok=True)

    # plots
    plot_latency_cdf(samples_df, report_dir / "latency_cdf.png")
    plot_throughput(samples_df, report_dir / "throughput.png")

    # index.html
    generate_html(report_dir, run)

    print(f"Report written to {report_dir}/index.html")


if __name__ == "__main__":
    main()
