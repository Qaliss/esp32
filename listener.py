import csv
import json
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import threading
import time
import uvicorn

app = FastAPI(title="IMU Data Collector", description="Collects and stores IMU data from multiple devices.")

class SensorData(BaseModel):
    accel_x: float
    accel_y: float
    accel_z: float
    gyro_x: float
    gyro_y: float
    gyro_z: float
    temp: float

class IMUDataPayload(BaseModel):
    device_id: str
    timestamp: int
    sensor: SensorData

CSV_FILENAME = "imu_data.csv"
BUFFER_SIZE = 100
data_buffer = []
buffer_lock = threading.Lock()

CSV_HEADERS = [
    'timestamp', 'device_id', 'system_time',
    'accel_x', 'accel_y', 'accel_z',
    'gyro_x', 'gyro_y', 'gyro_z',
    'temp'
]

def initialize_csv():
    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(CSV_HEADERS)
        print(f"Created new CSV file: {CSV_FILENAME}")
    else:
        print(f"CSV file already exists: {CSV_FILENAME}")

def write_buffer_to_csv():
    global data_buffer
    with buffer_lock:
        if len(data_buffer) > 0:
            with open(CSV_FILENAME, 'a', newline='') as file:
                writer = csv.writer(file)
                for row in data_buffer:
                    writer.writerow(row)

            print(f"Wrote {len(data_buffer)} data points to CSV")
            data_buffer = []

def auto_save_thread():
    while True:
        time.sleep(5)
        if len(data_buffer) > 0:
            write_buffer_to_csv()

@app.post("/data")
async def receive_imu_data(payload: IMUDataPayload):
    try:

        system_time = datetime.now().isoformat()
        
        csv_row = [
            payload.timestamp,
            payload.device_id,
            system_time,
            payload.sensor.accel_x, payload.sensor.accel_y, payload.sensor.accel_z,
            payload.sensor.gyro_x, payload.sensor.gyro_y, payload.sensor.gyro_z,
            payload.sensor.temp
        ]

        with buffer_lock:
            data_buffer.append(csv_row)

        if len(data_buffer) >= BUFFER_SIZE:
            write_buffer_to_csv()
        
        return {"status": "success", "message": "Data received"}
    except Exception as e:
        print(f"Error processing data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@app.get("/")
async def root():
    return {"message": "IMU Data Collector is running."}

@app.post("/save")
async def force_save():
    write_buffer_to_csv()
    return {"message": "Data buffer saved to CSV"}

if __name__ == "__main__":
    print("Starting IMU Data Collector...")

    initialize_csv()

    save_thread = threading.Thread(target=auto_save_thread, daemon=True)
    save_thread.start()

    print("Server starting on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)