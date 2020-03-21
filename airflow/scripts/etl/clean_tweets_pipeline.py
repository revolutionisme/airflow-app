#!/usr/bin/env python3

import sys
from components.data_processing import preprocess
import logging

input=sys.argv[1]
output=sys.argv[2]

logging.info("Starting data cleaning...")
preprocess(input,output)
logging.info("Completed data cleaning!")
