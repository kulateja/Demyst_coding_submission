# Use an official Python base image
FROM python:3.9-slim-buster 

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY /app/requirements.txt ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY /app /app
VOLUME /output /output

# Define the command to run your Python script
CMD ["python", "app.py"]