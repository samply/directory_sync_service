#!/bin/bash

cd /tmp

# Check if the directory exists and either pull or clone as appropriate.
if [ -d "data-loader" ]; then
    echo "'data-loader' exists. Pulling latest changes..."
    cd data-loader
    git pull
else
    echo "'data-loader' does not exist. Cloning the repository..."
    git clone https://github.com/samply/data-loader.git
    cd data-loader
fi

echo "Building Docker image..."
docker build -t samply/data-loader .

