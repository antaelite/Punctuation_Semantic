import csv
import matplotlib.pyplot as plt

# Read CSV data
processed_tuples = []
current_state_size = []

with open("./analysis/duplicate_elimination_metrics.csv", "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["processed_tuples"]:
            processed_tuples.append(int(row["processed_tuples"]))
            current_state_size.append(int(row["current_state_size"]))

# Create plot with both metrics on same scale
plt.figure(figsize=(12, 6))
plt.plot(
    range(len(processed_tuples)),
    processed_tuples,
    "b-",
    linewidth=2,
    label="Processed Tuples",
    alpha=0.7,
)
plt.plot(
    range(len(current_state_size)),
    current_state_size,
    "r-",
    linewidth=2.5,
    label="Current State Size",
)

plt.xlabel("Time (tuple sequence)", fontsize=12, fontweight="bold")
plt.ylabel("Count", fontsize=12, fontweight="bold")
plt.title(
    "Processed Tuples vs Current State Size\n(Punctuation keeps state bounded)",
    fontsize=14,
    fontweight="bold",
)
plt.legend(fontsize=11)
plt.grid(True, alpha=0.3)
plt.tight_layout()

plt.savefig("./analysis/state_size_graph.png", dpi=300, bbox_inches="tight")
print(
    f"Graph saved. Total tuples: {max(processed_tuples)}, Max state size: {max(current_state_size)}"
)
plt.show()
