#!/bin/bash

# Function to run the Python script
run_script() {
    input=$1
    /Users/bulat/Library/Caches/pypoetry/virtualenvs/bami-T1bVtQL0-py3.10/bin/python /Users/bulat/projects/main_bami/simulations/lz/basic_simulation.py "$input"
}

# Iterate over the input numbers from 1 to 10
for ((input=1; input<=10; input++)); do
    run_script "$input" &
done

# Wait for all the background processes to finish
wait

echo "All processes finished."
