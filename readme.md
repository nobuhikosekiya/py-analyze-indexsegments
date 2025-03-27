# Elasticsearch Index Management Tools

This repository contains a set of Python tools for analyzing and managing Elasticsearch indices, with a focus on segment management.

## Features

- **Fetch Elasticsearch Stats**: Get detailed statistics about your Elasticsearch indices
- **Analyze Index Segments**: Generate reports on segment counts across indices
- **Force Merge Indices**: Optimize indices by merging segments with performance metrics

## Setup

### Prerequisites

- Python 3.8 or higher
- Elasticsearch cluster access

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/elasticsearch-index-tools.git
   cd elasticsearch-index-tools
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Copy the environment example file and set your Elasticsearch connection details:
   ```
   cp .env.example .env
   ```

4. Edit the `.env` file with your Elasticsearch credentials:
   ```
   # For standard connection
   ES_URL=https://your-elasticsearch-host.com
   ES_PORT=9200
   
   # For Elastic Cloud
   # ES_CLOUD_ID=your-cloud-id
   
   # Authentication (choose one)
   ES_API_KEY=your-api-key
   # ES_USERNAME=elastic
   # ES_PASSWORD=changeme
   ```

## Usage

### Fetching Elasticsearch Stats

Retrieve statistics from your Elasticsearch cluster and save them to a local file:

```bash
python fetch_es_stats.py
```

This will produce a `stats.json` file with detailed information about your Elasticsearch indices.

### Analyzing Index Segments

Process the stats data to analyze segment counts across your indices:

```bash
python analyze.py
```

This will generate:
- Console output with segment counts by index and by prefix
- `index_segments_sorted.csv`: Segment counts for each index
- `index_segments_grouped_sorted.csv`: Aggregated segment counts by index prefix

### Force Merging Indices

Optimize your indices by merging segments and collect performance metrics:

```bash
# Force merge all indices
python force_merge_indices.py

# Force merge to a specific number of segments (e.g., 1)
python force_merge_indices.py --max-segments 1

# Only expunge deleted documents
python force_merge_indices.py --expunge-deletes

# Target specific indices with a pattern
python force_merge_indices.py --index-pattern "logstash-*"

# Combine multiple options
python force_merge_indices.py --max-segments 1 --index-pattern "logstash-*"
```

The force merge operation will:
1. Collect metrics before the operation
2. Perform the force merge
3. Collect metrics after the operation
4. Generate comparison reports

#### Generated Files

Each force merge operation generates three files with timestamps:
- `metrics_before_merge_TIMESTAMP.json`: Metrics before the operation
- `metrics_after_merge_TIMESTAMP.json`: Metrics after the operation
- `forcemerge_comparison_TIMESTAMP.json`: Detailed comparison showing the impact of the operation

## Metrics Collected

### For Each Node
- JVM heap usage (bytes and percentage)

### For Each Index
- Size in bytes
- Segment count

## Error Handling

The force merge script is designed to handle timeouts gracefully:
- If a force merge operation times out, the script will still collect post-operation metrics
- Comparison reports will indicate that the operation did not complete successfully
- This allows you to analyze partial results even when operations time out

## Notes

- Force merge operations can be resource-intensive and may take a long time for large indices
- The scripts use a 5-minute timeout by default, which can be adjusted in the code if needed
- For production clusters, consider running force merge operations during off-peak hours