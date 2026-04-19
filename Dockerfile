# Use a slim Python image to keep it lightweight
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script into the container
COPY main.py .

# Command to run the application
CMD ["python", "main.py"]
