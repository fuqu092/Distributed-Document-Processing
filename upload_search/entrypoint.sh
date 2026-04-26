#!/bin/sh

if [ "$1" = "--test" ]; then
    echo "Running test.py"
    python test.py
else
    echo "Running main.py"
    python main.py
fi