#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

filename=$1
echo "Running: $filename"
for i in {1..5}; do
    python3 $filename --no-cuda | grep "Self CPU time total:"
done