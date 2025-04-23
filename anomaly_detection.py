import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, RobustScaler
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense
import numpy as np
import matplotlib.pyplot as plt

# Load data
data = pd.read_csv('processed_data.csv')

# Preprocess data
scaler = RobustScaler()
data_scaled = scaler.fit_transform(data)

# Isolation Forest (First-Level Filter)
iforest = IsolationForest(n_estimators=200, contamination=0.05, random_state=42)
iforest.fit(data_scaled)
y_pred_iforest = iforest.predict(data_scaled)

# Filter out obvious anomalies
data_filtered = data_scaled[y_pred_iforest == 1]

# Autoencoder (Second-Level Filter)
input_dim = data_filtered.shape[1]
encoding_dim = int(input_dim * 0.5)
input_layer = Input(shape=(input_dim,))
encoder = Dense(encoding_dim, activation='relu')(input_layer)
decoder = Dense(input_dim, activation='linear')(encoder)
autoencoder = Model(inputs=input_layer, outputs=decoder)
autoencoder.compile(optimizer='adam', loss='mean_squared_error')
autoencoder.fit(data_filtered, data_filtered, epochs=100, batch_size=64, verbose=0, validation_split=0.2)

# Anomaly detection using Autoencoder
y_pred_autoencoder = autoencoder.predict(data_scaled)
reconstruction_error = np.mean((data_scaled - y_pred_autoencoder) ** 2, axis=1)
threshold = np.percentile(reconstruction_error, 95)
y_pred_autoencoder = np.where(reconstruction_error > threshold, -1, 1)

# Combine predictions
y_pred_final = np.where(y_pred_iforest == -1, -1, y_pred_autoencoder)

# Evaluate performance
anomaly_indices = np.where(y_pred_final == -1)[0]
print(f'Anomaly indices: {anomaly_indices}')

# Visualization
plt.figure(figsize=(10, 6))
plt.plot(reconstruction_error, label='Reconstruction Error')
plt.axhline(y=threshold, color='r', linestyle='--', label='Threshold')
plt.scatter(anomaly_indices, reconstruction_error[anomaly_indices], color='g', label='Anomalies')
plt.legend()
plt.title('Anomaly Detection')
plt.xlabel('Sample Index')
plt.ylabel('Reconstruction Error')
plt.show()

# Save anomaly indices to a file
np.savetxt('anomaly_indices.txt', anomaly_indices, fmt='%d')