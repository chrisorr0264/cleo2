# Use an official Python runtime as a parent image
FROM python:3.12

# Set environment variables to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /usr/src/app

# Install necessary apt packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    cmake \
    libopencv-dev \
    libsm6 \
    libxext6 \
    libxrender-dev \
    ffmpeg \
    libpq-dev \
    libmagickwand-dev \
    libboost-all-dev \
    libgeos-dev \
    wget \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and build dlib
RUN git clone https://github.com/davisking/dlib.git \
    && cd dlib \
    && mkdir build \
    && cd build \
    && cmake .. \
    && cmake --build . \
    && cd .. \
    && python3 setup.py install \
    && cd .. \
    && rm -rf dlib

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variables if needed
ENV NAME World

# Run the Python script when the container launches
CMD ["python", "process_file.py"]
