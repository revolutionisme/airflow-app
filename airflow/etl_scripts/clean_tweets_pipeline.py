#!/usr/bin/env python3

import sys
from components.data_processing import preprocess
import logging

#     The locations of the source and the destination files in the local
#     filesystem is provided as an first and second arguments to the
#     transformation script. The transformation script is expected to read the
#     data from source, transform it and write the output to the local
#     destination file. The operator then takes over control and uploads the
#     local destination file to S3.

input=sys.argv[1]
output=sys.argv[2]

logging.info("Starting data cleaning...")
preprocess(input,output)
logging.info("Completed data cleaning!")
