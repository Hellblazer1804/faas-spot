import os
import json
import matplotlib.pyplot as plt

def plot_heft_scaling(workflow_name, output_dir):
    metadata_path = os.path.join(output_dir, workflow_name, "metadata.json")

    if not os.path.exists(metadata_path):
        print(f"❌ File not found: {metadata_path}")
        return

    with open(metadata_path) as f:
        metadata = json.load(f)

    tasks = list(metadata.keys())
    ranks = [metadata[t]["heft_rank"] for t in tasks]
    scale_factors = [metadata[t]["scaling_factor"] for t in tasks]
    scaled = [metadata[t]["is_scalable"] for t in tasks]
    bar_colors = ['green' if s else 'red' for s in scaled]

    # Create plot
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.bar(tasks, ranks, color=bar_colors, label='HEFT Rank')
    ax1.set_xlabel("Tasks")
    ax1.set_ylabel("HEFT Rank", color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.set_ylim(0, max(ranks) + 2)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    # Line plot for scaling factor
    ax2 = ax1.twinx()
    ax2.plot(tasks, scale_factors, color='blue', marker='o', linestyle='--', label='Scaling Factor')
    ax2.set_ylabel("Scaling Factor", color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')
    ax2.set_ylim(0, max(scale_factors) + 1)

    plt.title(f"{workflow_name}: HEFT Rank (bars) vs Scaling Factor (line)\nGreen = Scaled, Red = Not Scaled")
    fig.tight_layout()

    # Save to file
    output_path = os.path.join(output_dir, workflow_name, "heft_scaling_plot.png")
    plt.savefig(output_path)
    print(f"✅ Plot saved to: {output_path}")

# Example usage
if __name__ == "__main__":
    workflow = input("Enter workflow name (e.g., wf-1): ").strip()
    os.chdir("..")
    output_dir = "generated-workflows"
    plot_heft_scaling(workflow, output_dir)
