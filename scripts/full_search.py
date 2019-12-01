#!/usr/bin/env python
# encoding: utf-8
"""
Search a Twitter archive (from archive.org) to find the characters which
occur before and after a chosen target.

-p  : Path to the Twitter archive
-d  : How many days to search (for testing)
-hr : How many hours to search (for testing)
"""
import argparse
import bz2
import json
import multiprocessing
import os
import pandas as pd
from unicode_codes import EMOJI_UNICODE
from timeit import default_timer as timer
from tqdm import tqdm
from twitter_search_funcs import find_context, sum_dicts

# Character to match
match = EMOJI_UNICODE[':pistol:']

# Global counters
counter_total_tweets = 0
counter_total_match = 0
counter_total_before = 0
counter_total_after = 0
counterdict_before = {}
counterdict_after = {}
counterdict_lang = {}


def worker(filename):
    """The worker function, invoked in a process. 'filename' is a
    zipped file of tweets to process. Results will be returned in
    'result_dict'

    Args:
        filename (str): Zipped file of tweets to process

    Returns:
        dict: result_dict
    """
    result_dict = {}
    result_dict['counter_total_tweets'] = 0
    result_dict['counter_total_match'] = 0
    result_dict['counter_total_before'] = 0
    result_dict['counter_total_after'] = 0
    result_dict['counterdict_before'] = {}
    result_dict['counterdict_after'] = {}
    result_dict['counterdict_lang'] = {}

    fbz = bz2.BZ2File(filename, 'rb')
    try:
        fdec = fbz.read()
    except IOError:
        return None
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
        result_dict['counter_total_tweets'] += 1
        if match in tweet['text']:
            result_dict['counter_total_match'] += 1
            result = find_context(tweet['text'], match)

            if result[0] in EMOJI_UNICODE.values():
                result_dict['counter_total_before'] += 1

                if result[0] in result_dict['counterdict_before'].keys():
                    result_dict['counterdict_before'][result[0]] += 1
                else:
                    result_dict['counterdict_before'][result[0]] = 1

            if result[2] in EMOJI_UNICODE.values():
                result_dict['counter_total_after'] += 1

                if result[2] in result_dict['counterdict_after'].keys():
                    result_dict['counterdict_after'][result[2]] += 1
                else:
                    result_dict['counterdict_after'][result[2]] = 1

            try:
                if tweet['lang'] in result_dict['counterdict_lang'].keys():
                    result_dict['counterdict_lang'][tweet['lang']] += 1
                else:
                    result_dict['counterdict_lang'][tweet['lang']] = 1
            except KeyError:
                continue

    return result_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Search a Twitter archive (from archive.org) to find the "
                    "characters which occur before and after a chosen target",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-p', '--data_path',
        default="/your/data/path/archive-twitter-2016-08/",
        help="Path to the Twitter archive")
    parser.add_argument(
        '-d', '--days', type=int, default=31,
        help="How many days to search (for testing)")
    parser.add_argument(
        '-hr', '--hours', type=int, default=24,
        help="How many hours to search (for testing)")
    args = parser.parse_args()

    number_of_processes = multiprocessing.cpu_count()
    multiprocessing.freeze_support()  # Prevent an error on Windows

    # Main search loop
    start_t = timer()
    all_files = []
    for day in range(args.days):
        day_str = "{:02d}".format(day + 1)
        for hour in range(args.hours):
            hour_str = "{:02d}".format(hour)
            file_path = os.path.join(args.data_path, day_str, hour_str)
            files = os.listdir(file_path)

            for i, f in enumerate(files):
                files[i] = os.path.join(file_path, f)

            all_files.extend(files)

    pool = multiprocessing.Pool(number_of_processes)
    try:
        for result_dict in tqdm(pool.imap_unordered(worker,
                                                    all_files,
                                                    chunksize=10),
                                total=len(all_files),
                                unit="files"):

            if result_dict is None:
                continue

            counter_total_tweets += result_dict['counter_total_tweets']
            counter_total_match += result_dict['counter_total_match']
            counter_total_before += result_dict['counter_total_before']
            counter_total_after += result_dict['counter_total_after']
            counterdict_before = sum_dicts(
                counterdict_before,
                result_dict['counterdict_before'])
            counterdict_after = sum_dicts(
                counterdict_after,
                result_dict['counterdict_after'])
            counterdict_lang = sum_dicts(
                counterdict_lang,
                result_dict['counterdict_lang'])

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    finally:
        pool.terminate()
        pool.join()

    end_t = timer()

    # Print outputs
    print("Elapsed Time    : {:.2f} min".format((end_t - start_t) / 60))
    print("Total Tweets    : {:d}".format(counter_total_tweets))
    print("Total Matches   : {:d}".format(counter_total_match))
    print("Total w/ Before : {:d}".format(counter_total_before))
    print("Total w/ After  : {:d}".format(counter_total_after))

    # Convert output to dataframe
    df_before = pd.DataFrame(
        list(counterdict_before.items()), columns=['Emoji', 'CountBefore'])
    df_after = pd.DataFrame(
        list(counterdict_after.items()), columns=['Emoji', 'CountAfter'])
    df_lang = pd.DataFrame(
        list(counterdict_lang.items()), columns=['Lang', 'Count'])

    # Merge before and after dataframes
    df_all = pd.merge(df_before, df_after, on='Emoji', how='outer')

    df_all.sort_values('CountBefore', ascending=False).head()
    df_all.sort_values('CountAfter', ascending=False).head()
    df_lang.sort_values('Count', ascending=False).head()
    df_all.sort_values('CountBefore',
                       ascending=False).head(20).plot.bar(y='CountBefore')
    df_all.sort_values('CountAfter',
                       ascending=False).head(20).plot.bar(y='CountAfter')

    # Export results as CSV files
    df_all.to_csv("./alldata.csv", encoding="utf-8")
    df_lang.to_csv("./langdata.csv", encoding="utf-8")
