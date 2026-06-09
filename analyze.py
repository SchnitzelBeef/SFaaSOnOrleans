import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import glob, os


def getPlotData(file):
    df = pd.read_csv(file)

    start_cols = [c for c in df.columns if "start" in c]
    end_cols = [c for c in df.columns if "end" in c]

    start_df = df.melt(
        id_vars=["epoch", "thread"],
        value_vars=start_cols,
        var_name="op_start",
        value_name="start_ns",
    )
    end_df = df.melt(
        id_vars=["epoch", "thread"],
        value_vars=end_cols,
        var_name="op_end",
        value_name="end_ns",
    )

    start_df["op_idx"] = start_df["op_start"].str.extract(r'(\d+)').astype(int)
    end_df["op_idx"] = end_df["op_end"].str.extract(r'(\d+)').astype(int)

    df = pd.merge(
        start_df.drop(columns=["op_start"]),
        end_df.drop(columns=["op_end"]),
        on=["epoch", "thread", "op_idx"]
    ).dropna()
    df = df.sort_values(by=["epoch", "thread", "op_idx"])

    df["latency_ms"] = (df["end_ns"] - df["start_ns"]) / 1_000_000.0

    global_min_ns = df["start_ns"].min()
    df["start_ms"] = (df["start_ns"] - global_min_ns) / 1_000_000.0

    max_thread = df["thread"].max()
    df["Thread Type"] = np.where(df["thread"] == max_thread, "End-To-End", "Workload")

    bin_size_ms = 1000.0
    df["time_bin_ms"] = (df["start_ms"] // bin_size_ms) * bin_size_ms

    epoch_level_stats = (
        df.groupby(["time_bin_ms", "Thread Type", "epoch"])
        .agg(
            avg_latency_ms=("latency_ms", "mean"),
            p95_latency_ms=("latency_ms", lambda x: np.percentile(x, 95)),
            total_ops=("latency_ms", "count"),
        )
        .reset_index()
    )
    epoch_level_stats["epoch_throughput"] = epoch_level_stats["total_ops"] / (bin_size_ms / 1000.0)

    stats = (
        epoch_level_stats.groupby(["time_bin_ms", "Thread Type"])
        .agg(
            avg_latency_ms=("avg_latency_ms", "mean"),
            p95_latency_ms=("p95_latency_ms", "mean"),
            std_latency_ms=("avg_latency_ms", "std"),
            total_throughput=("epoch_throughput", "mean"),
            std_throughput=("epoch_throughput", "std"),
        )
        .reset_index()
    )

    return stats

def color_rotation():
    colors = [
        "#FF2010", "#1060FF", "#10A020", "#FF8C00",
        "#8B00FF", "#00CED1", "#FF1493", "#FFD700"
    ]
    index = 0
    while True:
        yield colors[index % len(colors)]
        index += 1

def plotGraph(select : str):

    fig0, axes0 = plt.subplots(1, 1, figsize=(9, 8), sharex=True)
    fig1, axes1 = plt.subplots(1, 1, figsize=(9, 8), sharex=True)

    color_gen = color_rotation()

    for file in glob.glob(select + "-*_latencies.csv"):
        print("Processing file: " + file)

        stats = getPlotData(file)
        #numCustomerActors, numProductActors = " ", " "
        numCustomerActors, numProductActors = file.removeprefix(select + "-").removesuffix("_latencies.csv").split("-")
        legend = f"[{numCustomerActors}-{numProductActors}]"

        cat_data = stats[stats["Thread Type"] == "End-To-End"].sort_values("time_bin_ms")

        current_color = next(color_gen)

        ax_lat = axes0
        ax_lat.plot(
            cat_data["time_bin_ms"],
            cat_data["avg_latency_ms"],
            label=f"{legend}-Avg",
            color=current_color,
            linewidth=2,
            marker="o"
        )
        ax_lat.plot(
            cat_data["time_bin_ms"],
            cat_data["p95_latency_ms"],
            label=f"{legend}-P95",
            color=current_color,
            linestyle="--",
            linewidth=1.5,
            marker="x",
            alpha=0.85
        )
        ax_lat.fill_between(
            cat_data["time_bin_ms"],
            np.maximum(0, cat_data["avg_latency_ms"] - cat_data["std_latency_ms"]),
            cat_data["avg_latency_ms"] + cat_data["std_latency_ms"],
            color=current_color,
            alpha=0.15,
            label=f"{legend}-Latency Std Dev"
        )

        ax_lat.set_title(f"End-To-End Latency (Averaged Across Epochs)", fontsize=13, fontweight='bold')
        ax_lat.set_ylabel("Latency [ms]", fontsize=11)
        ax_lat.legend(loc="upper right")


        cat_data = stats[stats["Thread Type"] == "Workload"].sort_values("time_bin_ms")
        ax_tput = axes1
        ax_tput.plot(
            cat_data["time_bin_ms"],
            cat_data["total_throughput"],
            label=f"{legend}-Total",
            color=current_color,
            linewidth=2,
            marker="s"
        )
        ax_tput.fill_between(
            cat_data["time_bin_ms"],
            np.maximum(0, cat_data["total_throughput"] - cat_data["std_throughput"]),
            cat_data["total_throughput"] + cat_data["std_throughput"],
            color=current_color,
            alpha=0.15,
            label=f"{legend}-Std Dev"
        )

        ax_tput.set_title(f"Workload Throughput (Averaged Across Epochs)", fontsize=13, fontweight='bold')
        ax_tput.set_ylabel("Throughput [ops/s]", fontsize=11)
        ax_tput.set_xlabel("Time [ms]", fontsize=11)
        ax_tput.legend(loc="upper right")


    fig0.savefig(select + "_latency-graph.png")
    plt.close(fig0)

    fig1.savefig(select + "_throughput-graph.png")
    plt.close(fig1)

# Use this to select the number of proxies
# So "10" means it selects a file 10-x-y_latencies.csv, 
# where x and y are num customers and num products respectively
plotGraph("10")
