#!/bin/bash

mamba create -n ask-fsdl python==3.10

mamba activate ask-fsdl

pip install -r requirements-dev.txt