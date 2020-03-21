from components.data_cleaning import tweet_cleaning_for_sentiment_analysis
import csv
import json
#import nltk
#nltk.download('punkt')

#####################################################################################
#
# DATA PROCESSING
#
#####################################################################################

# def transform_instance(row):
#     cur_row = []
#     #Prefix the index-ed label with __label__
#     label = "__label__" + row[4]  
#     cur_row.append(label)
#     cur_row.extend(nltk.word_tokenize(tweet_cleaning_for_sentiment_analysis(row[2].lower())))
#     return cur_row


def transform_tweet(tweet_json, num_cols):
    row = [""] * num_cols
    row[0] = tweet_json["created_at"]
    row[1] = tweet_json["user"]["screen_name"]
    row[2] = tweet_cleaning_for_sentiment_analysis(tweet_json["text"].lower())
    row[3] = tweet_json["user"]["location"]
    row[4] = tweet_json["favorite_count"]
    row[5] = tweet_json["retweet_count"]
    row[6] = "" # default value for sentiment
    return row



def preprocess(input_file, output_file, keep=1):
    with open(input_file, 'r', encoding='utf-8') as f, open(output_file, 'w', encoding='utf-8') as csv_output_file:
        csv_writer = csv.writer(csv_output_file, delimiter='|', lineterminator='\n')
        # write the column row
        column_headers = ["created_at","screen_name","text","location","favorite_count","retweet_count","sentiment"]
        num_cols = len(column_headers)
        csv_writer.writerow(column_headers)
        line = f.readline()
        while line:
            tweet_json = json.loads(line)
            row = transform_tweet(tweet_json, num_cols)
            csv_writer.writerow(row)
            line = f.readline()
        csv_output_file.flush()
        f.flush()