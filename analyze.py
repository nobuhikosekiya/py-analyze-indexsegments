import json
import pandas as pd
import re

# Load data from local file 'stats.json'
with open("stats.json", "r", encoding="utf-8") as file:
    data = json.load(file)

# Get segment count for each index
index_segments = []
total_segments = 0  # Variable to store the total segment count
grouped_segments = {}

# Regular expression to identify dates (e.g., exclude "-2024.10.30-000080" part)
date_pattern = re.compile(r"(-\d{4}\.\d{2}\.\d{2}-\d{6})$")

for index_name, index_data in data.get("indices", {}).items():
    segment_count = index_data.get("primaries", {}).get("segments", {}).get("count", 0)

    # Get the prefix part
    prefix = date_pattern.sub("", index_name)

    # Data for individual indices
    index_segments.append({"Index Name": index_name, "Segment Count": segment_count})
    total_segments += segment_count  # Add segment count to total

    # Group and aggregate segment count and index count
    if prefix not in grouped_segments:
        grouped_segments[prefix] = {"Total Segment Count": 0, "Index Count": 0}
    
    grouped_segments[prefix]["Total Segment Count"] += segment_count
    grouped_segments[prefix]["Index Count"] += 1

# Create DataFrame and sort by Index Name
df_indices = pd.DataFrame(index_segments).sort_values(by="Index Name")

# Convert grouped data to DataFrame and sort by Segment count in descending order
df_groups = pd.DataFrame(
    [(prefix, data["Total Segment Count"], data["Index Count"]) for prefix, data in grouped_segments.items()],
    columns=["Index Prefix", "Total Segment Count", "Index Count"]
).sort_values(by="Total Segment Count", ascending=False)

# Display results
print("\n=== Segment Count by Index ===")
print(df_indices.to_string(index=False))

print("\nTotal Segment Count:", total_segments)

print("\n=== Total Segment Count and Index Count by Prefix (Descending) ===")
print(df_groups.to_string(index=False))

# Save as CSV (if needed)
df_indices.to_csv("index_segments_sorted.csv", index=False, encoding="utf-8")
df_groups.to_csv("index_segments_grouped_sorted.csv", index=False, encoding="utf-8")