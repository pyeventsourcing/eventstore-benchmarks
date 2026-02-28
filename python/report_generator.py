import argparse
import json
from collections import defaultdict
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
    lat_ms = samples.loc[samples["ok"], "latency_us"].astype(float) / 1000.0
    lat_ms = lat_ms.clip(lower=1e-3)
    lat_sorted = np.sort(lat_ms.values)
    p = np.linspace(0, 100, len(lat_sorted), endpoint=False)
    plt.figure(figsize=(6, 4))
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
    t0 = samples["t_ms"].min()
    df = samples.copy()
    df["t_rel_ms"] = df["t_ms"] - t0
    df["bucket"] = (df["t_rel_ms"] // 100).astype(int)
    grp = df.groupby("bucket")["ok"].apply(lambda x: int(x.sum()))
    eps = grp * 10
    plt.figure(figsize=(6, 4))
    plt.plot(eps.index.values / 10.0, eps.values)
    plt.xlabel("Time (s)")
    plt.ylabel("Events/sec")
    plt.title("Throughput over time")
    plt.grid(True, ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_comparison_latency_cdf(run_data, title, out_path: Path):
    """Plot latency CDF comparing stores for a specific writer count."""
    plt.figure(figsize=(8, 5))
    for label, samples_df in run_data:
        lat_ms = samples_df.loc[samples_df["ok"], "latency_us"].astype(float) / 1000.0
        lat_ms = lat_ms.clip(lower=1e-3)
        lat_sorted = np.sort(lat_ms.values)
        p = np.linspace(0, 100, len(lat_sorted), endpoint=False)
        plt.plot(lat_sorted, p, label=label)
    plt.xscale("log")
    plt.xlabel("Latency (ms) [log]")
    plt.ylabel("Percentile (%)")
    plt.title(title)
    plt.legend()
    plt.grid(True, which="both", ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_comparison_throughput(run_data, title, out_path: Path):
    """Plot throughput over time comparing stores for a specific writer count."""
    plt.figure(figsize=(8, 5))
    for label, samples_df in run_data:
        t0 = samples_df["t_ms"].min()
        df = samples_df.copy()
        df["t_rel_ms"] = df["t_ms"] - t0
        df["bucket"] = (df["t_rel_ms"] // 100).astype(int)
        grp = df.groupby("bucket")["ok"].apply(lambda x: int(x.sum()))
        eps = grp * 10
        plt.plot(eps.index.values / 10.0, eps.values, label=label)
    plt.xlabel("Time (s)")
    plt.ylabel("Events/sec")
    plt.title(title)
    plt.legend()
    plt.grid(True, ls=":")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_throughput_scaling(runs, out_path: Path):
    """Plot throughput vs writer count, one line per adapter."""
    # Group by adapter → list of (writers, throughput)
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 1)
        throughput = s["throughput_eps"]
        adapter_data[adapter].append((writers, throughput))

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        tps = [p[1] for p in points]
        plt.plot(ws, tps, marker="o", label=adapter)
    plt.xlabel("Writers")
    plt.ylabel("Throughput (events/sec)")
    plt.title("Throughput Scaling by Writer Count")
    plt.legend()
    plt.grid(True, ls=":")
    plt.xticks(sorted({s["summary"].get("writers", 1) for s in runs}))
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_p99_scaling(runs, out_path: Path):
    """Plot p99 latency vs writer count, one line per adapter."""
    adapter_data = defaultdict(list)
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        writers = s.get("writers", 1)
        p99 = s["latency"]["p99_ms"]
        adapter_data[adapter].append((writers, p99))

    plt.figure(figsize=(8, 5))
    for adapter, points in sorted(adapter_data.items()):
        points.sort()
        ws = [p[0] for p in points]
        p99s = [p[1] for p in points]
        plt.plot(ws, p99s, marker="o", label=adapter)
    plt.xlabel("Writers")
    plt.ylabel("p99 Latency (ms)")
    plt.title("p99 Latency Scaling by Writer Count")
    plt.legend()
    plt.grid(True, ls=":")
    plt.xticks(sorted({s["summary"].get("writers", 1) for s in runs}))
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_container_metrics(runs, out_path: Path):
    """Create a dramatic visualization of container resource usage."""
    # Collect data for each unique adapter (use dict to deduplicate and aggregate)
    adapter_data = {}

    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        container = s.get("container", {})

        # Only include if we have meaningful data
        if not (container.get("image_size_bytes") or container.get("peak_cpu_percent")):
            continue

        if adapter not in adapter_data:
            adapter_data[adapter] = {
                "image_size": 0,
                "startup_time": 0,
                "peak_cpu": 0,
                "peak_mem": 0,
                "count": 0
            }

        # Accumulate data (we'll use max for peaks, average for others)
        data = adapter_data[adapter]
        data["image_size"] = max(data["image_size"], container.get("image_size_bytes", 0) / (1024 * 1024))
        data["startup_time"] += container.get("startup_time_s", 0)
        data["peak_cpu"] = max(data["peak_cpu"], container.get("peak_cpu_percent", 0))
        data["peak_mem"] = max(data["peak_mem"], container.get("peak_memory_bytes", 0) / (1024 * 1024))
        data["count"] += 1

    if not adapter_data:
        return

    # Extract lists for plotting
    adapters = sorted(adapter_data.keys())
    image_sizes = [adapter_data[a]["image_size"] for a in adapters]
    startup_times = [adapter_data[a]["startup_time"] / adapter_data[a]["count"] for a in adapters]  # Average
    peak_cpus = [adapter_data[a]["peak_cpu"] for a in adapters]
    peak_mems = [adapter_data[a]["peak_mem"] for a in adapters]

    # Create a 2x2 subplot for dramatic effect
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Container Resource Metrics Comparison", fontsize=16, fontweight='bold')

    colors = plt.cm.Set3(np.linspace(0, 1, len(adapters)))

    # 1. Image Size - Vertical bar chart
    bars1 = ax1.bar(adapters, image_sizes, color=colors, edgecolor='black', linewidth=1.5)
    ax1.set_ylabel("Image Size (MB)", fontweight='bold')
    ax1.set_title("Container Image Size", fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars1, image_sizes):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.0f}', ha='center', va='bottom', fontweight='bold')

    # 2. Startup Time - Vertical bar chart
    bars2 = ax2.bar(adapters, startup_times, color=colors, edgecolor='black', linewidth=1.5)
    ax2.set_ylabel("Startup Time (seconds)", fontweight='bold')
    ax2.set_title("Container Startup Time", fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars2, startup_times):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.2f}s', ha='center', va='bottom', fontweight='bold')

    # 3. Peak CPU - Vertical bar chart
    bars3 = ax3.bar(adapters, peak_cpus, color=colors, edgecolor='black', linewidth=1.5)
    ax3.set_ylabel("Peak CPU (%)", fontweight='bold')
    ax3.set_title("Peak CPU Usage", fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    ax3.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars3, peak_cpus):
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.1f}%', ha='center', va='bottom', fontweight='bold')

    # 4. Peak Memory - Vertical bar chart
    bars4 = ax4.bar(adapters, peak_mems, color=colors, edgecolor='black', linewidth=1.5)
    ax4.set_ylabel("Peak Memory (MB)", fontweight='bold')
    ax4.set_title("Peak Memory Usage", fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    ax4.set_xticklabels(adapters, rotation=45, ha='right')
    for bar, v in zip(bars4, peak_mems):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'{v:.0f}', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight')
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


def generate_consolidated_html(out_base: Path, runs, writer_groups):
    # Summary table
    summary_rows = ""
    for run in runs:
        s = run["summary"]
        adapter = s["adapter"]
        workload = Path(s["workload"]).stem
        writers = s.get("writers", "?")
        report_link = f"report-{adapter}-{workload}/index.html"

        # Get container metrics
        container = s.get("container", {})
        startup_time = f"{container.get('startup_time_s', 0):.1f}s" if container.get("startup_time_s") else "N/A"
        image_size_mb = f"{container.get('image_size_bytes', 0) / 1024 / 1024:.0f}" if container.get("image_size_bytes") else "N/A"

        # CPU metrics (avg / peak)
        avg_cpu = container.get("avg_cpu_percent")
        peak_cpu = container.get("peak_cpu_percent")
        cpu_display = "N/A"
        if avg_cpu is not None and peak_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}% / {peak_cpu:.1f}%"
        elif avg_cpu is not None:
            cpu_display = f"{avg_cpu:.1f}%"

        # Memory metrics (avg / peak in MB)
        avg_mem = container.get("avg_memory_bytes")
        peak_mem = container.get("peak_memory_bytes")
        mem_display = "N/A"
        if avg_mem is not None and peak_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f} / {peak_mem / 1024 / 1024:.0f}"
        elif avg_mem is not None:
            mem_display = f"{avg_mem / 1024 / 1024:.0f}"

        summary_rows += f"""
      <tr>
        <td><a href='{report_link}'>{adapter}</a></td>
        <td>{workload}</td>
        <td>{writers}</td>
        <td>{s['duration_s']:.1f}s</td>
        <td>{s['throughput_eps']:.0f}</td>
        <td>{s['latency']['p50_ms']:.2f}</td>
        <td>{s['latency']['p99_ms']:.2f}</td>
        <td>{image_size_mb}</td>
        <td>{startup_time}</td>
        <td>{cpu_display}</td>
        <td>{mem_display}</td>
      </tr>"""

    # Per-writer-count comparison sections
    comparison_sections = ""
    for wc in sorted(writer_groups.keys()):
        comparison_sections += f"""
    <h2>Writers = {wc}</h2>
    <div class='row'>
      <div class='card'>
        <h3>Latency CDF</h3>
        <img src='comparison_w{wc}_latency_cdf.png' width='560'>
      </div>
      <div class='card'>
        <h3>Throughput over time</h3>
        <img src='comparison_w{wc}_throughput.png' width='560'>
      </div>
    </div>"""

    # Container metrics section
    container_section = """
    <h2>Container Resource Metrics</h2>
    <div class='card' style='max-width: 100%;'>
      <img src='container_metrics.png' style='width: 100%; max-width: 1200px;'>
    </div>"""

    # Scaling charts (only if multiple writer counts)
    scaling_section = ""
    if len(writer_groups) > 1:
        scaling_section = """
    <h2>Scaling</h2>
    <div class='row'>
      <div class='card'>
        <h3>Throughput vs Writers</h3>
        <img src='scaling_throughput.png' width='560'>
      </div>
      <div class='card'>
        <h3>p99 Latency vs Writers</h3>
        <img src='scaling_p99.png' width='560'>
      </div>
    </div>"""

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset='utf-8'>
  <title>ESBS Consolidated Report</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    h1, h2, h3 {{ margin-top: 1.2rem; }}
    table {{ border-collapse: collapse; margin: 1rem 0; }}
    th, td {{ border: 1px solid #ddd; padding: 0.5rem 1rem; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .row {{ display: flex; gap: 1rem; flex-wrap: wrap; }}
    .card {{ border: 1px solid #eee; border-radius: 8px; padding: 1rem; }}
  </style>
</head>
<body>
  <h1>ESBS Consolidated Report</h1>
  <h2>Summary</h2>
  <table>
    <tr><th>Adapter</th><th>Workload</th><th>Writers</th><th>Duration</th><th>Throughput (eps)</th><th>p50 (ms)</th><th>p99 (ms)</th><th>Image (MB)</th><th>Startup</th><th>CPU (avg/peak)</th><th>Mem MB (avg/peak)</th></tr>
    {summary_rows}
  </table>
  {container_section}
  {scaling_section}
  {comparison_sections}
</body>
</html>
"""
    with open(out_base / "index.html", "w") as f:
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

    # Generate individual reports for each run
    for run in runs:
        samples_df = pd.DataFrame(run["samples"])
        run["_samples_df"] = samples_df
        adapter = run["summary"]["adapter"]
        workload = Path(run["summary"]["workload"]).stem

        report_dir = out_base / f"report-{adapter}-{workload}"
        report_dir.mkdir(parents=True, exist_ok=True)

        plot_latency_cdf(samples_df, report_dir / "latency_cdf.png")
        plot_throughput(samples_df, report_dir / "throughput.png")
        generate_html(report_dir, run)
        print(f"Report written to {report_dir}/index.html")

    # Group runs by writer count for per-group comparison charts
    writer_groups = defaultdict(list)
    for run in runs:
        wc = run["summary"].get("writers", 1)
        adapter = run["summary"]["adapter"]
        writer_groups[wc].append((adapter, run["_samples_df"]))

    # Generate per-writer-count comparison charts
    for wc, run_data in sorted(writer_groups.items()):
        if len(run_data) > 1:
            plot_comparison_latency_cdf(
                run_data,
                f"Latency CDF — {wc} writer(s)",
                out_base / f"comparison_w{wc}_latency_cdf.png",
            )
            plot_comparison_throughput(
                run_data,
                f"Throughput — {wc} writer(s)",
                out_base / f"comparison_w{wc}_throughput.png",
            )
        elif len(run_data) == 1:
            # Single store at this writer count — still generate charts for consistency
            plot_comparison_latency_cdf(
                run_data,
                f"Latency CDF — {wc} writer(s)",
                out_base / f"comparison_w{wc}_latency_cdf.png",
            )
            plot_comparison_throughput(
                run_data,
                f"Throughput — {wc} writer(s)",
                out_base / f"comparison_w{wc}_throughput.png",
            )

    # Generate scaling summary charts (throughput & p99 vs writers)
    if len(writer_groups) > 1:
        plot_throughput_scaling(runs, out_base / "scaling_throughput.png")
        plot_p99_scaling(runs, out_base / "scaling_p99.png")

    # Generate container metrics visualization
    plot_container_metrics(runs, out_base / "container_metrics.png")

    generate_consolidated_html(out_base, runs, writer_groups)
    print(f"Consolidated report written to {out_base}/index.html")


if __name__ == "__main__":
    main()
