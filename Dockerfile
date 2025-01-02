# Use an official Python image
FROM python:3.11.4-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements_ingest.txt /app/requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . /app

# Expose any necessary ports (optional, e.g., if using FastAPI or Flask)
# EXPOSE 8000

# Define the default command to run the application
CMD ["python", "main.py"]
