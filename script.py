import requests
import pandas as pd
import datetime
from sklearn.preprocessing import StandardScaler

def get_prometheus_metrics(query):
    prometheus_url = "http://localhost:8080"
    end_time = int(datetime.datetime.now().timestamp())
    start_time = end_time - 1000  # 300 seconds = 5 minutes
    params = {
        'query': query,
        'start': start_time,
        'end': end_time,
        'step': '15'
    }
    response = requests.get(f"{prometheus_url}/api/v1/query_range", params=params)
    data = response.json()
    result_list = []
    if 'data' in data and 'result' in data['data']:
        for result in data['data']['result']:
            for value in result['values']:
                result_list.append({'timestamp': value[0], 'value': value[1]})
    df = pd.DataFrame(result_list)
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df.set_index('timestamp', inplace=True)
    return df

def get_logs():
    log_data = [
        {"timestamp": "2024-01-20 10:00:00", "level": "INFO", "message": "Request processed successfully"},
        {"timestamp": "2024-01-20 10:01:00", "level": "ERROR", "message": "Service unavailable"},
        {"timestamp": "2024-01-20 10:02:00", "level": "INFO", "message": "Database query executed"},
    ]
    df = pd.DataFrame(log_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)

    return df

def process_data():
    cpu_usage = get_prometheus_metrics('rate(node_cpu_seconds_total{mode!="idle"}[5m])')
    memory_usage = get_prometheus_metrics('node_memory_MemTotal_bytes - node_memory_MemFree_bytes')
    request_latency = get_prometheus_metrics('http_request_duration_seconds_sum / http_request_duration_seconds_count')
    network_receive_bytes = get_prometheus_metrics('rate(node_network_receive_bytes_total[5m])')
    network_transmit_bytes = get_prometheus_metrics('rate(node_network_transmit_bytes_total[5m])')

    print("CPU Usage:")
    print(cpu_usage)
    print("Memory Usage:")
    print(memory_usage)
    print("Request Latency:")
    print(request_latency)
    print("Network Receive Bytes:")
    print(network_receive_bytes)
    print("Network Transmit Bytes:")
    print(network_transmit_bytes)

    dataframes = [cpu_usage, memory_usage, network_receive_bytes, network_transmit_bytes]
    dataframes = [df for df in dataframes if not df.empty]

    if dataframes:
        # Ensure all dataframes contain numeric data
        for df in dataframes:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df.dropna(inplace=True)

        # Resample dataframes to a common frequency
        freq = '15S'  # 15 seconds
        dataframes = [df.resample('15s').mean() for df in dataframes]

        # Rename columns
        dataframes[0] = dataframes[0].rename(columns={'value': 'cpu_usage'})
        dataframes[1] = dataframes[1].rename(columns={'value': 'memory_usage'})
        dataframes[2] = dataframes[2].rename(columns={'value': 'network_receive_bytes'})
        dataframes[3] = dataframes[3].rename(columns={'value': 'network_transmit_bytes'})

        # Merge dataframes
        aggregated_data = pd.concat(dataframes, axis=1)

        # Interpolate missing values
        aggregated_data = aggregated_data.interpolate(method='linear')

        scaler = StandardScaler()
        normalized_data = scaler.fit_transform(aggregated_data.fillna(0))

        return normalized_data
    else:
        return None

if __name__ == "__main__":
    processed_data = process_data()
    if processed_data is not None:
        print(processed_data)
        pd.DataFrame(processed_data).to_csv('processed_data.csv', index=False)
    else:
        print("No data to process.")