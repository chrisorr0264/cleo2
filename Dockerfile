# Use an official Python runtime as a parent image
FROM python:3.12

# Set environment variables to avoid prompts
ENV DEBIAN_FRONTEND=noninteractive

# Set the working directory in the container
WORKDIR /usr/src/app

# Install necessary apt packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    cmake \
    libboost-all-dev \
    libopencv-dev \
    python3-opencv \
    ffmpeg \
    imagemagick \
    libmagickwand-dev \
    exiftool \
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


# Run the Python script when the container launches
ENTRYPOINT ["python", "-c", "from file_processor import FileProcessor; import os; file_info = tuple(os.getenv('NEW_FILE').split(',')); FileProcessor(file_info)"]
