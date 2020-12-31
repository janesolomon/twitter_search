#!/usr/bin/env python
# encoding: utf-8
"""
Search a Twitter archive (from archive.org) to find the characters which
occur before and after a chosen target. Also counts total number of emoji characters.

-p  : Path to the Twitter archive
-d  : How many days to search (for testing)
-hr : How many hours to search (for testing)
"""
import argparse
import multiprocessing
from timeit import default_timer as timer

import pandas as pd
from tqdm import tqdm

from twitter_search import find_all, find_context, sum_dicts
from twitter_search.data import get_all_files, read_zip, unpack_files
from twitter_search.unicode_codes import EMOJI_UNICODE


class Results:

    """Search results data class.

    Attributes:
        counter_total_after (int): Total number of tweets with emoji after the match character
        counter_total_before (int): Total number of tweets with emoji before the match character
        counter_total_match (int): Total number of tweets with the match character
        counter_total_tweets (int): Total number of tweets
        counter_total_tweets_wemoji (int): Total number of tweets with any emoji
        counterdict_after (dict): Distribution of emoji after the match character
        counterdict_before (dict): Distribution of emoji before the match character
        counterdict_lang (dict): Distribution of tweet languages
        counterdict_all_emoji (dict): Distribution of all emoji
    """

    def __init__(self):
        """Initialize all counters to 0 and counter dicts to be empty."""
        self.counter_total_tweets = 0
        self.counter_total_tweets_wemoji = 0
        self.counter_total_match = 0
        self.counter_total_before = 0
        self.counter_total_after = 0

        self.counterdict_before = {}
        self.counterdict_after = {}
        self.counterdict_lang = {}
        self.counterdict_all_emoji = {}


def worker(filename):
    """The worker function, invoked in a process.

    Args:
        filename (str): Zipped file of tweets to process

    Returns:
        Results
    """
    results = Results()

    for tweet in read_zip(filename):

        # Count total number of tweets
        results.counter_total_tweets += 1

        # Count total numbers of emoji in tweet
        all_emoji, all_count = find_all(tweet["text"])
        if not all_emoji:
            continue
        results.counter_total_tweets_wemoji += 1
        for i, c in enumerate(all_emoji):
            if c in results.counterdict_all_emoji.keys():
                results.counterdict_all_emoji[c] += all_count[i]
            else:
                results.counterdict_all_emoji[c] = all_count[i]

        # Count number and context of match emoji
        if MATCH in all_emoji:
            results.counter_total_match += 1
            result = find_context(tweet["text"], MATCH)

            # Before match
            if result[0] in EMOJI_UNICODE.values():
                results.counter_total_before += 1

                if result[0] in results.counterdict_before.keys():
                    results.counterdict_before[result[0]] += 1
                else:
                    results.counterdict_before[result[0]] = 1
            # After match
            if result[2] in EMOJI_UNICODE.values():
                results.counter_total_after += 1

                if result[2] in results.counterdict_after.keys():
                    results.counterdict_after[result[2]] += 1
                else:
                    results.counterdict_after[result[2]] = 1

            try:
                if tweet["lang"] in results.counterdict_lang.keys():
                    results.counterdict_lang[tweet["lang"]] += 1
                else:
                    results.counterdict_lang[tweet["lang"]] = 1
            except KeyError:
                continue

    return results


def parse_cli_args():
    """Parse the require CLI arguments for the run.

    Returns:
        argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="Search a Twitter archive (from archive.org)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-p",
        "--data_path",
        default="/your/data/path/archive-twitter-2016-08/",
        help="Path to the Twitter archive",
    )
    parser.add_argument(
        "-d", "--days", type=int, default=31, help="How many days to search (for testing)"
    )
    parser.add_argument(
        "-hr", "--hours", type=int, default=24, help="How many hours to search (for testing)"
    )
    parser.add_argument(
        "-u", "--unpack", default=False, action="store_true", help="Unpack tar files"
    )
    return parser.parse_args()


def run():
    """Run the full search.

    Returns:
        Results
    """
    start_t = timer()
    # Global counters
    results_global = Results()
    # Set multiprocessing cpu count
    number_of_processes = multiprocessing.cpu_count()
    multiprocessing.freeze_support()  # Prevent an error on Windows
    # Create pool of processes
    pool = multiprocessing.Pool(number_of_processes)
    try:
        # Run worker functions and use tqdm progress bar
        processes = pool.imap_unordered(worker, all_files, chunksize=10)
        for results in tqdm(processes, total=len(all_files), unit="files"):
            if results is None:
                continue
            # Update all global counters
            results_global.counter_total_tweets += results.counter_total_tweets
            results_global.counter_total_tweets_wemoji += results.counter_total_tweets_wemoji
            results_global.counter_total_match += results.counter_total_match
            results_global.counter_total_before += results.counter_total_before
            results_global.counter_total_after += results.counter_total_after
            results_global.counterdict_before = sum_dicts(
                results_global.counterdict_before, results.counterdict_before
            )
            results_global.counterdict_after = sum_dicts(
                results_global.counterdict_after, results.counterdict_after
            )
            results_global.counterdict_lang = sum_dicts(
                results_global.counterdict_lang, results.counterdict_lang
            )
            results_global.counterdict_all_emoji = sum_dicts(
                results_global.counterdict_all_emoji, results.counterdict_all_emoji
            )

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    finally:
        pool.terminate()
        pool.join()
    end_t = timer()
    # Print outputs of the search run
    print("Elapsed Time          : {:.2f} min".format((end_t - start_t) / 60))
    print("Total Tweets          : {:d}".format(results_global.counter_total_tweets))
    print("Total Tweets w/ Emoji : {:d}".format(results_global.counter_total_tweets_wemoji))
    print("Total Matches         : {:d}".format(results_global.counter_total_match))
    print("Total w/ Before       : {:d}".format(results_global.counter_total_before))
    print("Total w/ After        : {:d}".format(results_global.counter_total_after))

    return results_global


def save_results(results):
    """Save results to csv."""
    # Convert output to dataframe
    df_before = pd.DataFrame(
        list(results.counterdict_before.items()), columns=["Emoji", "CountBefore"]
    )
    df_after = pd.DataFrame(
        list(results.counterdict_after.items()), columns=["Emoji", "CountAfter"]
    )
    df_lang = pd.DataFrame(list(results.counterdict_lang.items()), columns=["Lang", "Count"])
    df_allemoji = pd.DataFrame(
        list(results.counterdict_all_emoji.items()), columns=["Emoji", "Count"]
    )

    # Merge before and after dataframes
    df_all = pd.merge(df_before, df_after, on="Emoji", how="outer")

    # Export results as CSV files
    df_all.to_csv("./alldata.csv", encoding="utf-8")
    df_lang.to_csv("./langdata.csv", encoding="utf-8")
    df_allemoji.to_csv("./allemojidata.csv", encoding="utf-8")


if __name__ == "__main__":

    args = parse_cli_args()

    # Character to match
    MATCH = EMOJI_UNICODE[":pistol:"]

    # Unpack and list all files
    if args.unpack:
        unpack_files(args.data_path)
    all_files = get_all_files(args.data_path, days=args.days, hours=args.hours)

    # Main search loop
    RESULTS_GLOBAL = run()
    save_results(RESULTS_GLOBAL)
