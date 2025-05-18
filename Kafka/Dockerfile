# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script into the container at /app
COPY data_provider.py .

# Run data_provider.py when the container launches
CMD ["python", "./data_provider.py"]
