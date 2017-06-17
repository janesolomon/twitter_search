import bz2
import json
import multiprocessing
import os
import pandas as pd
from collections import Counter
from unicode_codes import EMOJI_UNICODE
from timeit import default_timer as timer
from twitter_search_funcs import find_context, progress

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


def sum_dicts(a, b):
    """Merge dictionaries, summing their values.
    For example:
    a = {"x": 1, "y": 1}
    b = {"x": 1, "z": 1}
    returns {"x": 2, "y": 1, "z": 1}
    """
    return Counter(a) + Counter(b)


def worker(filename):
    """ The worker function, invoked in a process. 'filename' is a
        zipped file of tweets to process. Results will be retuned in
        'result_dict'
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
    number_of_processes = multiprocessing.cpu_count()
    multiprocessing.freeze_support()  # Prevent an error on Windows

    # Main search loop
    start_t = timer()
    for day in range(31):
        day_str = "{:02d}".format(day + 1)
        for hour in range(24):
            hour_str = "{:02d}".format(hour)
            file_path = os.path.join(data_path, day_str, hour_str)
            files = os.listdir(file_path)

            for i, f in enumerate(files):
                files[i] = os.path.join(file_path, f)

            pool = multiprocessing.Pool(number_of_processes)
            results = pool.map(worker, files)
            pool.close()
            pool.join()

            for i, result_dict in enumerate(results):

                progress(i + hour * len(results),
                         len(results) * 24,
                         suffix="Searching Day {}, Hour {}".format(
                            day_str, hour_str))

                if result_dict is not None:
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
