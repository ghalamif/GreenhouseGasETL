#!/bin/bash


if command -v python3 &>/dev/null; then
    python3 pipeline.py

elif command -v python &>/dev/null; then
    python pipeline.py

else
    echo "Python is not installed."
    exit 1
fi