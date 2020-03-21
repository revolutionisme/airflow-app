from components.data_cleaning import tweet_cleaning_for_sentiment_analysis
import json
#import nltk
#nltk.download('punkt')

"""
For each line - Read each json object, extract the desired fields, 
clean tweet text and write as new json object.
"""

def transform_tweet(tweet_json, json_form):
    json_form["created_at"] = tweet_json["created_at"]
    json_form["screen_name"] = tweet_json["user"]["screen_name"]
    json_form["text"] = tweet_cleaning_for_sentiment_analysis(tweet_json["text"].lower())
    json_form["location"] = tweet_json["user"]["location"]
    json_form["favorite_count"] = tweet_json["favorite_count"]
    json_form["retweet_count"] = tweet_json["retweet_count"]
    return json_form


def preprocess(input_file, output_file, keep=1):
    with open(input_file, 'r', encoding='utf-8') as f, open(output_file, 'w', encoding='utf-8') as output:
        line = f.readline()
        while line:
            json_form = {
                "created_at": "","screen_name": "","text": "",
                "location": "","favorite_count": "","retweet_count": ""
            }
            tweet_json = json.loads(line)
            row = transform_tweet(tweet_json, json_form)
            output.write(json.dumps(row) + '\n')
            line = f.readline()
        output.close()
        f.close()