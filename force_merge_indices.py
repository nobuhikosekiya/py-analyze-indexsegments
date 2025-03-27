import os
import time
import argparse
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

def connect_to_elasticsearch():
    """
    Connect to Elasticsearch using credentials from environment variables.
    Returns an Elasticsearch client instance.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Common connection options with longer timeout
    connection_options = {
        "request_timeout": 300  # 5 minutes timeout
    }

    # Initialize Elasticsearch client based on configuration
    es_client = None

    # Check if using cloud_id
    cloud_id = os.getenv("ES_CLOUD_ID")
    if cloud_id:
        # Check authentication method
        api_key = os.getenv("ES_API_KEY")
        if api_key:
            # Connect using cloud_id and API key
            es_client = Elasticsearch(
                cloud_id=cloud_id,
                api_key=api_key,
                **connection_options
            )
        else:
            # Connect using cloud_id and username/password
            es_client = Elasticsearch(
                cloud_id=cloud_id,
                basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD")),
                **connection_options
            )
    else:
        # Connect using URL and port
        es_url = os.getenv("ES_URL", "http://localhost")
        es_port = os.getenv("ES_PORT", "9200")
        
        # Construct the base URL
        base_url = f"{es_url}:{es_port}"
        
        # Check authentication method
        api_key = os.getenv("ES_API_KEY")
        if api_key:
            # Connect using URL/port and API key
            es_client = Elasticsearch(
                hosts=[base_url],
                api_key=api_key,
                **connection_options
            )
        else:
            # Connect using URL/port and username/password
            es_client = Elasticsearch(
                hosts=[base_url],
                basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD")),
                **connection_options
            )

    # Verify connection
    if not es_client.ping():
        raise ConnectionError("Failed to connect to Elasticsearch")
    
    print(f"Successfully connected to Elasticsearch")
    return es_client

def get_heap_stats(client):
    """Get current JVM heap usage statistics"""
    nodes_stats = client.nodes.stats(metric=["jvm"])
    heap_stats = {}
    
    for node_id, node_data in nodes_stats.body.get('nodes', {}).items():
        node_name = node_data.get('name', node_id)
        # Access JVM memory data safely
        jvm_data = node_data.get('jvm', {})
        mem_data = jvm_data.get('mem', {})
        
        heap_stats[node_name] = {
            'heap_used_bytes': mem_data.get('heap_used_in_bytes', 0),
            'heap_used_percent': mem_data.get('heap_used_percent', 0),
            'heap_max_bytes': mem_data.get('heap_max_in_bytes', 0)
        }
    
    return heap_stats

def get_indices_stats(client, index_pattern=None):
    """Get current indices statistics including size and segment count"""
    indices_stats = client.indices.stats(index=index_pattern, metric=["store", "segments"])
    indices_data = {}
    
    for index_name, index_data in indices_stats.body['indices'].items():
        # Get store data safely with defaults
        store_data = index_data.get('total', {}).get('store', {})
        segments_data = index_data.get('total', {}).get('segments', {})
        
        # Use get() with defaults to handle missing fields
        indices_data[index_name] = {
            'size_bytes': store_data.get('size_in_bytes', 0),
            # Some ES versions might not include the pretty format, so handle it safely
            'size_pretty': store_data.get('size', f"{store_data.get('size_in_bytes', 0)} bytes"),
            'segment_count': segments_data.get('count', 0)
        }
    
    return indices_data

def save_metrics_to_file(metrics, filename):
    """Save metrics to a JSON file"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"Metrics saved to {filename}")

def force_merge_indices(max_num_segments=None, only_expunge_deletes=False, index_pattern=None):
    """
    Force merge all indices or specific indices matching a pattern.
    Records metrics before and after the operation.
    
    Args:
        max_num_segments (int, optional): Maximum number of segments to merge to.
        only_expunge_deletes (bool): Only expunge deleted documents.
        index_pattern (str, optional): Index pattern to match (e.g., "logstash-*").
            If None, all indices will be force-merged.
    """
    try:
        # Connect to Elasticsearch
        client = connect_to_elasticsearch()
        
        # Create a timestamp for this run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Record metrics before the operation
        print("Recording metrics before force merge...")
        heap_before = get_heap_stats(client)
        indices_before = get_indices_stats(client, index_pattern)
        
        # Save pre-operation metrics
        save_metrics_to_file({
            'timestamp': timestamp,
            'type': 'before_merge',
            'heap_stats': heap_before,
            'indices_stats': indices_before
        }, f"metrics_before_merge_{timestamp}.json")
        
        # Prepare parameters for force merge
        params = {}
        if max_num_segments is not None:
            params["max_num_segments"] = max_num_segments
        if only_expunge_deletes:
            params["only_expunge_deletes"] = True
            
        # Display operation information
        operation_type = "expunge deletes" if only_expunge_deletes else "force merge"
        segment_info = f" to {max_num_segments} segments" if max_num_segments else ""
        target_info = f" for indices matching '{index_pattern}'" if index_pattern else " for all indices"
        
        # Execute force merge with error handling
        force_merge_completed = False
        force_merge_error = None
        start_time = time.time()  # Move start_time initialization before the try block
        
        try:
            print(f"Starting {operation_type}{segment_info}{target_info}")
            
            # Execute force merge
            response = client.indices.forcemerge(
                index=index_pattern,
                **params
            )
            
            # If we get here, the operation completed successfully
            force_merge_completed = True
            elapsed_time = time.time() - start_time
            print(f"Force merge operation completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            # Capture the error but continue processing
            force_merge_error = str(e)
            elapsed_time = time.time() - start_time
            print(f"Force merge operation timed out or failed after {elapsed_time:.2f} seconds with error: {force_merge_error}")
            print("Proceeding with post-merge metrics collection anyway...")
        
        # Record metrics after the operation (even if it failed)
        print("Recording metrics after force merge attempt...")
        heap_after = get_heap_stats(client)
        indices_after = get_indices_stats(client, index_pattern)
        
        # Save post-operation metrics
        save_metrics_to_file({
            'timestamp': timestamp,
            'type': 'after_merge',
            'force_merge_completed': force_merge_completed,
            'force_merge_error': force_merge_error,
            'heap_stats': heap_after,
            'indices_stats': indices_after,
            'elapsed_time_seconds': elapsed_time
        }, f"metrics_after_merge_{timestamp}.json")
        
        # Generate comparison report
        comparison = {
            'timestamp': timestamp,
            'operation': {
                'type': 'force_merge',
                'max_num_segments': max_num_segments,
                'only_expunge_deletes': only_expunge_deletes,
                'index_pattern': index_pattern,
                'elapsed_time_seconds': elapsed_time,
                'completed': force_merge_completed,
                'error': force_merge_error
            },
            'heap_comparison': {},
            'indices_comparison': {}
        }
        
        # Compare heap stats
        for node, before_data in heap_before.items():
            after_data = heap_after.get(node, {})
            if after_data:
                heap_used_diff = after_data['heap_used_bytes'] - before_data['heap_used_bytes']
                heap_percent_diff = after_data['heap_used_percent'] - before_data['heap_used_percent']
                
                comparison['heap_comparison'][node] = {
                    'before': before_data,
                    'after': after_data,
                    'diff_bytes': heap_used_diff,
                    'diff_percent': heap_percent_diff
                }
        
        # Compare index stats
        for index, before_data in indices_before.items():
            after_data = indices_after.get(index, {})
            if after_data:
                size_diff = after_data['size_bytes'] - before_data['size_bytes']
                segment_diff = after_data['segment_count'] - before_data['segment_count']
                
                comparison['indices_comparison'][index] = {
                    'before': before_data,
                    'after': after_data,
                    'size_diff_bytes': size_diff,
                    'size_diff_percent': (size_diff / before_data['size_bytes']) * 100 if before_data['size_bytes'] > 0 else 0,
                    'segment_diff': segment_diff,
                    'segment_diff_percent': (segment_diff / before_data['segment_count']) * 100 if before_data['segment_count'] > 0 else 0
                }
        
        # Save comparison report
        save_metrics_to_file(comparison, f"forcemerge_comparison_{timestamp}.json")
        
        # Print summary
        print("\nOperation Summary:")
        if force_merge_completed:
            print(f"  - Operation completed successfully in {elapsed_time:.2f} seconds")
        else:
            print(f"  - Operation timed out or failed after {elapsed_time:.2f} seconds")
            print(f"  - Error: {force_merge_error}")
            print("  - Post-operation metrics were still collected")
            
        print(f"  - Affected indices: {len(comparison['indices_comparison'])}")
        
        total_size_before = sum(data['before']['size_bytes'] for data in comparison['indices_comparison'].values())
        total_size_after = sum(data['after']['size_bytes'] for data in comparison['indices_comparison'].values())
        total_size_diff = total_size_after - total_size_before
        total_size_diff_percent = (total_size_diff / total_size_before) * 100 if total_size_before > 0 else 0
        
        total_segments_before = sum(data['before']['segment_count'] for data in comparison['indices_comparison'].values())
        total_segments_after = sum(data['after']['segment_count'] for data in comparison['indices_comparison'].values())
        total_segments_diff = total_segments_after - total_segments_before
        total_segments_diff_percent = (total_segments_diff / total_segments_before) * 100 if total_segments_before > 0 else 0
        
        print(f"  - Total size before: {total_size_before:,} bytes")
        print(f"  - Total size after: {total_size_after:,} bytes")
        print(f"  - Size difference: {total_size_diff:,} bytes ({total_size_diff_percent:.2f}%)")
        print(f"  - Total segments before: {total_segments_before:,}")
        print(f"  - Total segments after: {total_segments_after:,}")
        print(f"  - Segment difference: {total_segments_diff:,} ({total_segments_diff_percent:.2f}%)")
        print(f"\nDetailed metrics saved to forcemerge_comparison_{timestamp}.json")
        
    except Exception as e:
        print(f"Error during force merge operation: {str(e)}")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Force merge Elasticsearch indices")
    parser.add_argument("--max-segments", type=int, help="Maximum number of segments to merge to")
    parser.add_argument("--expunge-deletes", action="store_true", help="Only expunge deleted documents")
    parser.add_argument("--index-pattern", type=str, help="Index pattern to match (e.g., 'logstash-*')")
    
    args = parser.parse_args()
    
    # Execute force merge with provided arguments
    force_merge_indices(
        max_num_segments=args.max_segments,
        only_expunge_deletes=args.expunge_deletes,
        index_pattern=args.index_pattern
    )