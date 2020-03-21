#!/usr/bin/env python3

from textblob import TextBlob
import sys
import logging
import json

"""
For each line - Read each json object, perform sentiment analysis on
the tweet text, add sentiment field to json object, append to parent json object 
and write the parent json to a file.
"""

def perform_sentiment_analysis(input, output):
	with open(output, 'w', encoding='utf-8') as output_file, open(input, 'r', encoding='utf-8') as input_file:
		line = input_file.readline()
		parent_json = {"tweets": []}
		while line:
			tweet_json = json.loads(line)
			analysis = TextBlob(tweet_json["text"])
			tweet_json["sentiment"] = str("{0:.5f}".format(analysis.sentiment.polarity))
			parent_json["tweets"].append(tweet_json)
			line = input_file.readline()
		output_file.write(json.dumps(parent_json))
		input_file.close()
		output_file.close()

input=sys.argv[1]
output=sys.argv[2]

logging.info("Starting sentiment analysis on tweets...")
perform_sentiment_analysis(input,output)
logging.info("Completed sentiment analysis!")
