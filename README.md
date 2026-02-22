# MLOps Pipeline

This repository contains a containerized Python application that executes deterministically, computing a rolling mean and signal generation for cryptocurrency OHLCV data.

## 1. Setup Instructions

It is very important to put the 'data.csv' in the project root directory.

To set up the environment locally, install the required dependencies
pip install -r requirements.txt

## 2. Local Execution Instructions
Execute the script using the following command:
python run.py --input data.csv --config config.yaml \ 
--output metrics.json --log-file run.log 

## 3. Docker Instructions
To build and run the containerized application:

# Build the Docker image 
docker build -t mlops-task . 

# Run the container 
docker run --rm mlops-task 

## 4. Expected Output
Upon successful execution, the application will generate a JSON output (`metrics.json`) with the following structure:
{
    "version": "v1",
    "rows_processed": 10000,
    "metric": "signal_rate",
    "value": 0.4990,
    "latency_ms": 127,
    "seed": 42,
    "status": "success"
}

## 5. Dependencies
The following Python packages are required:
- pandas
- numpy
- pyyaml
