#!/usr/bin/env python
# encoding: utf-8
import bz2
import json
import os
import pandas as pd
from timeit import default_timer as timer
from twitter_search_funcs import find_context, progress
from unicode_codes import EMOJI_UNICODE

data_path = "/your/data/path/archive-twitter-2016-08/"

# Character to match
match = EMOJI_UNICODE[':pistol:']

# Counters
counter_total_tweets = 0
counter_total_match = 0
counter_total_before = 0
counter_total_after = 0
counterdict_before = {}
counterdict_after = {}
counterdict_lang = {}

# Main search loop
start_t = timer()
for day in range(31):
    day_str = "{:02d}".format(day + 1)
    for hour in range(24):
        hour_str = "{:02d}".format(hour)
        file_path = os.path.join(data_path, day_str, hour_str)
        files = os.listdir(file_path)
        for i, f in enumerate(files):
            progress(i + hour * len(files),
                     len(files) * 24,
                     suffix="Searching Day {}, Hour {}".format(day_str, hour_str))
            fbz = bz2.BZ2File(os.path.join(file_path, f), 'rb')
            try:
                fdec = fbz.read()
            except IOError:
                continue
            finally:
                fbz.close()
            fdecutf = fdec.decode('utf-8')

            for t in fdecutf.split('\n'):
                try:
                    tweet = json.loads(t)
                except (ValueError):
                    continue
                if 'delete' in tweet.keys():
                    continue
                counter_total_tweets += 1
                if match in tweet['text']:
                    counter_total_match += 1
                    result = find_context(tweet['text'], match)

                    if result[0] in EMOJI_UNICODE.values():
                        counter_total_before += 1

                        if result[0] in counterdict_before.keys():
                            counterdict_before[result[0]] += 1
                        else:
                            counterdict_before[result[0]] = 1

                    if result[2] in EMOJI_UNICODE.values():
                        counter_total_after += 1

                        if result[2] in counterdict_after.keys():
                            counterdict_after[result[2]] += 1
                        else:
                            counterdict_after[result[2]] = 1

                    try:
                        if tweet['lang'] in counterdict_lang.keys():
                            counterdict_lang[tweet['lang']] += 1
                        else:
                            counterdict_lang[tweet['lang']] = 1
                    except KeyError:
                        continue
end_t = timer()

# Print outputs
print("Elapsed Time    : {:.2f} min".format((end_t - start_t) / 60))
print("Total Tweets    : {:d}".format(counter_total_tweets))
print("Total Matches   : {:d}".format(counter_total_match))
print("Total w/ Before : {:d}".format(counter_total_before))
print("Total w/ After  : {:d}".format(counter_total_after))

# Convert output to dataframe
df_before = pd.DataFrame(list(counterdict_before.items()), columns=['Emoji', 'CountBefore'])
df_after = pd.DataFrame(list(counterdict_after.items()), columns=['Emoji', 'CountAfter'])
df_lang = pd.DataFrame(list(counterdict_lang.items()), columns=['Lang', 'Count'])

# Merge before and after dataframes
df_all = pd.merge(df_before, df_after, on='Emoji', how='outer')

df_all.sort_values('CountBefore', ascending=False).head()
df_all.sort_values('CountAfter', ascending=False).head()
df_lang.sort_values('Count', ascending=False).head()
df_all.sort_values('CountBefore', ascending=False).head(20).plot.bar(y='CountBefore')
df_all.sort_values('CountAfter', ascending=False).head(20).plot.bar(y='CountAfter')

# Export results as CSV files
df_all.to_csv("./alldata.csv")
df_lang.to_csv("./langdata.csv")
