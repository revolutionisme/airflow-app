#!/usr/bin/env python3

import sys
import logging
import json
import datetime

input=sys.argv[1]
output=sys.argv[2]

# bucket jsons by topic
def map(json_array, part_m):
	for j in json_array:
		topic = j["topic"]
		if topic in part_m:
			part_m[topic].append(j)
		else:
			part_m[topic] = [j]
	return part_m

def reduce(part_m):
	results = []
	timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S:00")
	for topic_key in part_m:
		json_form = {"topic": "","timestamp": "","sentiment": "","maxNegText": "","maxPosText": ""}
		sentiment = float(0)
		neg_sent = float('inf')
		pos_sent = float('-inf')
		maxNegText = ""
		maxPosText = ""
		for j in part_m[topic_key]:
			sentiment+=float(j["sentiment"])
			if j["sentiment"] < neg_sent:
				neg_sent = j["sentiment"]
				maxNegText = j["text"]
			if j["sentiment"] > pos_sent:
				pos_sent = j["sentiment"]
				maxPosText = j["text"]
		json_form["topic"] = topic_key
		json_form["timestamp"] = timestamp
		json_form["sentiment"] = float(sentiment / len(part_m[topic_key]))
		json_form["maxNegText"] = maxNegText
		json_form["maxPosText"] = maxPosText
		results.append(json_form)
	return results


def aggregate_sentiment_results(input, output):
	logging.info("Aggregating results")
	with open(input, 'r', encoding='utf-8') as input_file:
		line = input_file.readline()
		parent_json = {"results": []}
		batch = []
		part_m = {}
		logging.info("Running map...")
		while line:
			tweet_json_object = json.loads(line)
			tweet_json_array = tweet_json_object["tweets"]
			for i in range(len(tweet_json_array)):
				if i % 500 == 0:
					map(batch, part_m)
					batch = []
				else:
					batch.append(tweet_json_array[i])
			line = input_file.readline()
		part_m = map(batch, part_m)
		input_file.close()
	logging.info("Map phase completed")

	with open(output, 'w', encoding='utf-8') as output_file:
		logging.info("Running reduce...")

		parent_json["results"] = reduce(part_m)

		logging.info("Reduce completed")
		logging.info("Writing results to file: %s", output_file.name)

		output_file.write(json.dumps(parent_json))

		output_file.close()
	logging.info("Finished writing results")
	

logging.info("Summarizing sentiment analysis results...")
aggregate_sentiment_results(input,output)
logging.info("Completed!")