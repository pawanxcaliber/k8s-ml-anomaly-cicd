# script.py - Full Repaired Code for data collection and separate file saving

import requests
import pandas as pd
import datetime
import numpy as np # Still useful for potential NaN/Inf handling

def get_prometheus_metrics(query, prometheus_url="http://localhost:8080", time_window_seconds=3600, step='15s'):
    """
    Fetches time series data from Prometheus, handling multiple series per query
     and ensuring unique timestamp+label combinations.

    Args:
        query (str): The Prometheus query string.
        prometheus_url (str): The base URL for the Prometheus API.
        time_window_seconds (int): The time window in seconds ending now.
        step (str): The step size for the query_range.

    Returns:
        pd.DataFrame: DataFrame with timestamp as index, 'value' column,
                      and columns for Prometheus labels (like pod, namespace, container),
                      or empty DataFrame if query fails or returns no data or contains no results.
    """
    end_time = int(datetime.datetime.now().timestamp())
    start_time = end_time - time_window_seconds
    params = {
        'query': query,
        'start': start_time,
        'end': end_time,
        'step': step
    }
    print(f"Fetching query: {query}")
    try:
        response = requests.get(f"{prometheus_url}/api/v1/query_range", params=params, timeout=60) # Increased timeout
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()

        result_list = []
        if 'data' in data and 'result' in data['data']:
            for result in data['data']['result']:
                 metric_labels = result.get('metric', {}) # Get labels dictionary
                 for value in result['values']:
                     point_data = {
                         'timestamp': value[0],
                         'value': value[1]
                     }
                     for label_name, label_value in metric_labels.items():
                         point_data[label_name] = label_value
                     result_list.append(point_data)

        df = pd.DataFrame(result_list)

        if not df.empty:
            # Ensure timestamp and value are in correct formats
            df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce')

            # Drop rows where timestamp or value are NaN immediately
            df.dropna(subset=['timestamp', 'value'], inplace=True)

            if df.empty: # Check if empty after dropping NaNs
                print(f"Warning: No valid timestamp/value data remaining for query '{query}'.")
                return pd.DataFrame()

            # Convert timestamp to datetime after dropping NaNs
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

            # --- CORRECTED Duplicate Handling ---
            # Ensure uniqueness based on the combination of timestamp AND all label columns
            # This correctly handles multiple time series (distinguished by labels)
            # and duplicate points within a single series.
            id_cols = [col for col in df.columns if col not in ['timestamp', 'value']] # Identify label columns
            subset_cols = ['timestamp'] + id_cols # Define columns that uniquely identify a data point in a series at a time

            if df.duplicated(subset=subset_cols).any():
                duplicates_count = df.duplicated(subset=subset_cols).sum()
                print(f"Warning: Duplicate timestamp + label combinations found ({duplicates_count} duplicates) for query '{query}'. Keeping first occurrence for each unique combination.")
                df = df.drop_duplicates(subset=subset_cols, keep='first')
            # --- End CORRECTED ---

            # Set timestamp as index AFTER handling duplicates
            df.set_index('timestamp', inplace=True)

            # Return DataFrame with timestamp index, 'value' column, and all label columns
            # Use .copy() to avoid SettingWithCopyWarning later
            return df[['value'] + id_cols].copy()

        # Return empty DataFrame if result_list was empty initially
        return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Prometheus ({query}): {e}")
        return pd.DataFrame()
    except ValueError as e:
        # This might catch errors in response.json() or numeric conversion
        print(f"Error processing Prometheus response for query '{query}': {e}")
        return pd.DataFrame()
    except Exception as e:
         # Catch any other unexpected errors during processing
         print(f"An unexpected error occurred during data processing for query '{query}': {e}")
         return pd.DataFrame()


# get_logs function placeholder - will be implemented later
# def get_logs(...):
#     # Code to collect logs from a source (kubectl logs, logging agent files, API)
#     # Process into DataFrame with timestamp, pod, namespace, container, message, etc.
#     # return logs_df

# get_network_activity function placeholder - will be implemented later
# def get_network_activity(...):
#     # Code to collect network data from a source (network policy logs, service mesh API, etc.)
#     # Process into DataFrame with timestamp, source_pod, dest_ip, dest_port, bytes, etc.
#     # return network_df


def collect_and_save_data(time_window_seconds=3600, step='15s'):
    """
    Collects data from various sources and saves them to separate files.
    This version focuses on Node and Available Per-Pod metrics (CPU, Memory).
    """
    print(f"Starting data collection for the last {time_window_seconds} seconds with a step of 15s")

    # Define the step duration for resampling/asfreq
    step_duration = pd.Timedelta(step)

    # --- 1. Collect Node-Level Metrics ---
    print("\n--- Collecting Node-Level Metrics ---")
    # These queries are expected to return node aggregates.
    # If they return multiple series (e.g., per CPU core), get_prometheus_metrics
    # will return them with labels. For the 'node_metrics_raw.csv', we'll
    # simply aggregate them by timestamp (e.g., take the mean across labels)
    # to get one value per metric per timestamp for the node level.
    node_cpu_df = get_prometheus_metrics('rate(node_cpu_seconds_total{mode!="idle"}[5m])', time_window_seconds=time_window_seconds, step=step)
    node_memory_df = get_prometheus_metrics('node_memory_MemTotal_bytes - node_memory_MemFree_bytes', time_window_seconds=time_window_seconds, step=step)
    node_network_receive_df = get_prometheus_metrics('rate(node_network_receive_bytes_total[5m])', time_window_seconds=time_window_seconds, step=step)
    node_network_transmit_df = get_prometheus_metrics('rate(node_network_transmit_bytes_total[5m])', time_window_seconds=time_window_seconds, step=step)


    # List of fetched node metric dataframes and desired column names
    node_metrics_data = {
        'cpu_usage': node_cpu_df,
        'memory_usage': node_memory_df,
        'network_receive_bytes': node_network_receive_df,
        'network_transmit_bytes': node_network_transmit_df
    }

    # Aggregate node data: take the mean value for each timestamp across any labels
    # This reduces multiple series for the same metric (like per CPU core) into a single time series per metric name
    node_aggregated_df_list = []
    for name, df in node_metrics_data.items():
        if not df.empty and 'value' in df.columns:
            # Group by timestamp (index) and take the mean of the 'value'
            # This handles cases where a 'node' query might return multiple series with labels
            agg_df = df.groupby(df.index)['value'].mean().rename(name).to_frame()
            node_aggregated_df_list.append(agg_df)
            print(f"Node metric '{name}' aggregated. Shape: {agg_df.shape}")
        else:
            print(f"Warning: Node metric '{name}' dataframe is empty or missing 'value' column. Skipping aggregation.")

    node_data_for_save = pd.DataFrame()
    if node_aggregated_df_list:
         # Join all aggregated node metric series together
         node_data_for_save = node_aggregated_df_list[0]
         for i in range(1, len(node_aggregated_df_list)):
             node_data_for_save = node_data_for_save.join(node_aggregated_df_list[i], how='outer')

         # Ensure a consistent time index and interpolate/fill NaNs for the final node data
         # Create a date range covering the period with the specified frequency
         if not node_data_for_save.empty:
              full_time_range = pd.date_range(start=node_data_for_save.index.min(), end=node_data_for_save.index.max(), freq=step_duration)
              # Reindex the aggregated data to this new dense index, filling missing timestamps
              # This will create NaNs where data was missing at the new frequency steps
              node_data_for_save = node_data_for_save.reindex(full_time_range)
              # Interpolate and fill NaNs
              node_data_for_save = node_data_for_save.interpolate(method='linear').fillna(method='bfill').fillna(method='ffill').fillna(0)
         else:
             # If list was not empty but resulting join was, ensure it's an empty DataFrame
             node_data_for_save = pd.DataFrame()


         if not node_data_for_save.empty:
              print(f"\nNode-level metrics aggregated and processed for saving. Shape: {node_data_for_save.shape}")
              print(node_data_for_save.head())
              print("...")
              print(node_data_for_save.tail())
         else:
              print("\nNo node-level metrics available for saving after processing.")
    else:
         print("\nNo node-level metric series were successfully aggregated.")


    # --- 2. Collect Per-Pod Metrics ---
    print("\n--- Collecting Per-Pod Metrics ---")
    # Use the WORKING queries identified during troubleshooting (CPU, Memory)
    # These queries *should* return data points with 'pod', 'namespace' labels.
    # get_prometheus_metrics is expected to return a DataFrame with timestamp index, 'value', and label columns.

    # Per-Pod CPU Usage (rate, total CPU per pod) - Verified Working
    pod_cpu_query = 'rate(container_cpu_usage_seconds_total{pod!="", namespace!="", cpu="total"}[5m])'
    pod_cpu_df = get_prometheus_metrics(pod_cpu_query, time_window_seconds=time_window_seconds, step=step)

    # Per-Pod Memory Usage (working set bytes per pod) - Verified Working
    pod_memory_query = 'container_memory_working_set_bytes{pod!="", namespace!=""}'
    pod_memory_df = get_prometheus_metrics(pod_memory_query, time_window_seconds=time_window_seconds, step=step)

    # Per-Pod Network Receive/Transmit - Queries returned NO DATA in Prometheus UI.
    # Keeping them here but expecting empty dataframes unless Prometheus setup changes.
    # If you find working queries for network metrics later, update these lines.
    # Note: Removed the cpu="total" filter as network metrics won't have it.
    pod_network_receive_query = 'rate(container_network_receive_bytes_total{pod!="", namespace!=""}[5m])' # Still likely no data
    pod_network_receive_df = get_prometheus_metrics(pod_network_receive_query, time_window_seconds=time_window_seconds, step=step)

    pod_network_transmit_query = 'rate(container_network_transmit_bytes_total{pod!="", namespace!=""}[5m])' # Still likely no data
    pod_network_transmit_df = get_prometheus_metrics(pod_network_transmit_query, time_window_seconds=time_window_seconds, step=step)

    # Note: Add more per-pod metrics here as needed (filesystem I/O, etc.)

    # List of fetched per-pod metric dataframes and desired column names
    pod_metrics_data = {
         'pod_cpu_usage': pod_cpu_df,
         'pod_memory_usage': pod_memory_df,
         'pod_network_receive_bytes': pod_network_receive_df, # Will likely be empty
         'pod_network_transmit_bytes': pod_network_transmit_df # Will likely be empty
         # Add more (name, df) tuples for other pod metrics
    }

    # Combine per-pod metrics into a single dataframe.
    # This dataframe should retain the timestamp index AND the label columns (namespace, pod, container).
    # Start with an empty dataframe and outer join subsequent ones.
    # We need to merge based on the full identifier: timestamp + all label columns.
    pod_data_for_save = pd.DataFrame()
    all_pod_label_cols = set() # Keep track of all label columns found across all pod metrics
    first_pod_metric_df = True

    for name, df in pod_metrics_data.items():
        if not df.empty and 'value' in df.columns:
             # Identify label columns in this specific dataframe returned by get_prometheus_metrics
             label_cols_in_df = [col for col in df.columns if col not in ['value']]
             all_pod_label_cols.update(label_cols_in_df) # Add to the set of all labels found

             # Select value and label columns, rename value column
             df_to_join = df[['value'] + label_cols_in_df].rename(columns={'value': name})

             # The DataFrame is indexed by timestamp. We need to join based on timestamp AND labels.
             # Easiest way is to reset index, join on timestamp and labels, then set timestamp back if needed later.
             df_to_join = df_to_join.reset_index() # Timestamp becomes a column

             if first_pod_metric_df:
                 pod_data_for_save = df_to_join
                 first_pod_metric_df = False
             else:
                 # Merge with the existing combined pod dataframe on 'timestamp' AND all known label columns
                 # Use outer join to keep all data points from all metrics for all series
                 # Define the columns to merge on (timestamp + all labels found so far)
                 merge_on_cols = ['timestamp'] + list(all_pod_label_cols)
                 # Ensure all merge_on_cols exist in both dataframes before merging
                 common_merge_cols = list(set(merge_on_cols) & set(pod_data_for_save.columns) & set(df_to_join.columns))

                 if common_merge_cols:
                     pod_data_for_save = pd.merge(pod_data_for_save, df_to_join, on=common_merge_cols, how='outer')
                 else:
                     # This case implies no common columns to merge on, unlikely if timestamp is present and labels are consistent
                     print(f"Warning: No common columns found to merge per-pod metric '{name}'. Skipping merge.")
                     pass # Skip this dataframe

        else:
            print(f"Warning: Per-pod metric '{name}' dataframe is empty or missing 'value' column. Skipping merge.")

    # Ensure all relevant label columns are present after merges, filling missing ones with a placeholder if necessary
    # This is important so the subsequent grouping/processing per series works correctly
    final_pod_label_cols = list(all_pod_label_cols)
    for col in final_pod_label_cols:
         if col not in pod_data_for_save.columns:
             # Add missing label column that was present in other metrics but not this one for a given row
             # Fill with a placeholder indicating it wasn't present for that row's original metric source
             # Use string 'N/A' as label columns are typically strings
             pod_data_for_save[col] = 'N/A' # Or pd.NA, depending on pandas version/preference

    # Interpolate and fill NaNs *after* joining all per-pod dataframes.
    # This needs to happen PER SERIES (per unique combination of labels).
    # Group by the final set of label columns and apply asfreq, interpolate, fillna.
    if not pod_data_for_save.empty:
         if final_pod_label_cols: # Only group if there are label columns present in the data
              print("\nProcessing merged per-pod metrics per series (interpolation, fillna)...")

              # Set multi-index (labels + timestamp) for processing per group
              # Ensure timestamp is a column before setting multi-index
              if 'timestamp' in pod_data_for_save.columns:
                   # Set multi-index: [label1, label2, ..., timestamp]
                   pod_data_for_save = pod_data_for_save.set_index(final_pod_label_cols + ['timestamp'])

                   # Group by the label levels of the multi-index
                   # The lambda function operates on the DataFrame for each group (series)
                   pod_data_for_save_processed = pod_data_for_save.groupby(level=list(range(len(final_pod_label_cols)))).apply(
                       # Within each group (each series), apply asfreq, interpolate, fillna on the timestamp index level
                       lambda x: x.droplevel(list(range(len(final_pod_label_cols)))).asfreq(step_duration).interpolate(method='linear').fillna(method='bfill').fillna(method='ffill').fillna(0)
                   )
                   # The above apply results in a DataFrame indexed by timestamp within each group.
                   # The group keys (labels) are added back as index levels automatically by apply.
                   # The index is now (label1, label2, ..., timestamp). We need to reset it to columns.
                   pod_data_for_save = pod_data_for_save_processed.reset_index()

                   # Ensure label columns are strings and fill any remaining NaNs with 'N/A'
                   for col in final_pod_label_cols:
                       if col in pod_data_for_save.columns:
                           pod_data_for_save[col] = pod_data_for_save[col].astype(str).fillna('N/A')

                   # Ensure metric value columns are numeric and fill any remaining NaNs with 0
                   metric_cols = [col for col in pod_data_for_save.columns if col not in ['timestamp'] + final_pod_label_cols]
                   for col in metric_cols:
                        pod_data_for_save[col] = pd.to_numeric(pod_data_for_save[col], errors='coerce').fillna(0)

              else:
                   print("Error: Timestamp column not found in per-pod data after initial merge. Cannot process per series.")
                   pod_data_for_save = pd.DataFrame() # Ensure it's empty

              if not pod_data_for_save.empty:
                   print(f"\nPer-pod metrics aggregated and processed for saving. Shape: {pod_data_for_save.shape}")
                   print(pod_data_for_save.head())
                   print("...")
                   print(pod_data_for_save.tail())
              else:
                   print("\nNo per-pod metrics available for saving after processing.")

         else: # No label columns in the merged data (unexpected for per-pod unless all queries failed or returned no labels)
             print("No label columns found in merged per-pod metrics. Applying interpolation/fillna as a single series.")
             if 'timestamp' in pod_data_for_save.columns:
                  pod_data_for_save = pod_data_for_save.set_index('timestamp').asfreq(step_duration).interpolate(method='linear').fillna(method='bfill').fillna(method='ffill').fillna(0).reset_index()
             # Final fillna for metrics and labels
             for col in pod_data_for_save.columns:
                  if col in final_pod_label_cols:
                       pod_data_for_save[col] = pod_data_for_save[col].astype(str).fillna('N/A')
                  else:
                       pod_data_for_save[col] = pd.to_numeric(pod_data_for_save[col], errors='coerce').fillna(0)

             if not pod_data_for_save.empty:
                 print(f"\nPer-pod metrics aggregated and processed for saving. Shape: {pod_data_for_save.shape}")
                 print(pod_data_for_save.head())
                 print("...")
                 print(pod_data_for_save.tail())
             else:
                 print("\nNo per-pod metrics available for saving after processing.")


    else:
         print("\nNo per-pod metrics available for saving after aggregation.")


    # --- 3. Collect Logs (Placeholder) ---
    print("\n--- Collecting Logs (Placeholder) ---")
    # This function would need to be implemented separately based on your log source
    # logs_df = get_logs(...)
    # print(f"Logs collected. Shape: {logs_df.shape}")
    logs_df = pd.DataFrame() # Placeholder empty dataframe


    # --- 4. Collect Network Activity (Placeholder) ---
    print("\n--- Collecting Network Activity (Placeholder) ---")
    # This function would need to be implemented separately based on your network source
    # network_df = get_network_activity(...)
    # print(f"Network activity collected. Shape: {network_df.shape}")
    network_df = pd.DataFrame() # Placeholder empty dataframe


    # --- 5. Save Data to Separate Files ---
    print("\n--- Saving Data to Separate Files ---")
    # Ensure output directories exist if saving to subfolders
    # os.makedirs('raw_data', exist_ok=True)

    if not node_data_for_save.empty:
        # Save with timestamp index for node metrics
        node_data_for_save.to_csv('node_metrics_raw.csv', index=True)
        print("Node-level metrics saved to 'node_metrics_raw.csv'")

    if not pod_data_for_save.empty:
        # Save per-pod data, keeping timestamp as a column and including labels
        pod_data_for_save.to_csv('per_pod_metrics_raw.csv', index=False)
        print("Per-pod metrics saved to 'per_pod_metrics_raw.csv'")

    if not logs_df.empty:
        logs_df.to_csv('container_logs_raw.csv', index=False)
        print("Container logs saved to 'container_logs_raw.csv'")

    if not network_df.empty:
        network_df.to_csv('network_flows_raw.csv', index=False)
        print("Network flows saved to 'network_flows_raw.csv'")

    print("\nData collection and saving complete.")


# --- Main Execution Block ---
if __name__ == "__main__":
    # Define data fetching parameters
    time_window_seconds = 3600 # Fetch 1 hour of data
    step_size = '15s' # Keep 15s step

    collect_and_save_data(time_window_seconds=time_window_seconds, step=step_size)