#!/usr/bin/env python
# encoding: utf-8
"""
Search a Twitter archive (from archive.org) to find all characters related to time.
Finds distributions of those and all other emoji in the tweets.

-p  : Path to the Twitter archive
-d  : How many days to search (for testing)
-hr : How many hours to search (for testing)
"""
import argparse
import multiprocessing
from timeit import default_timer as timer

import pandas as pd
from tqdm import tqdm

from twitter_search import find_all, find_all_if, sum_dicts
from twitter_search.data import get_all_files, read_zip, unpack_files
from twitter_search.unicode_codes import EMOJI_UNICODE


class Results:

    """Search results data class.

    Attributes:
        counter_total_match (int): Total number of tweets with a match character
        counter_total_tweets (int): Total number of tweets
        counter_total_tweets_wemoji (int): Total number of tweets with any emoji
        counterdict_lang (dict): Distribution of tweet languages
        counterdict_all_emoji (dict): Distribution of all emoji
        counterdict_all_emoji_if_match (dict): Distribution of all emoji when match is found
    """

    def __init__(self):
        """Initialize all counters to 0 and counter dicts to be empty."""
        self.counter_total_tweets = 0
        self.counter_total_tweets_wemoji = 0
        self.counter_total_match = 0

        self.counterdict_lang = {}
        self.counterdict_all_emoji = {}
        self.counterdict_all_emoji_if_match = {}

        self.counterdict_all_emoji_if_clockfaces = {}
        self.counterdict_all_emoji_if_hourglasses = {}
        self.counterdict_all_emoji_if_soon = {}
        self.counterdict_all_emoji_if_watch = {}
        self.counterdict_all_emoji_if_stopwatch = {}
        self.counterdict_all_emoji_if_mantelpiece_clock = {}
        self.counterdict_all_emoji_if_timer_clock = {}
        self.counterdict_all_emoji_if_alarm_clock = {}

    def add_to(self, key, val, attr):
        """Adds a value to an given dictionary key for a given attribiute of the class.

        Args:
            key (str)
            val (int)
            attr (str)
        """
        d = getattr(self, attr)
        if key in d.keys():
            d[key] += val
        else:
            d[key] = val
        setattr(self, attr, d)


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

        # Count total numbers of emoji in tweet when there is a match
        all_emoji, all_count = find_all_if(tweet["text"], MATCHES_ALL)
        if not all_emoji:
            continue
        results.counter_total_match += 1
        for i, c in enumerate(all_emoji):
            if c in results.counterdict_all_emoji_if_match.keys():
                results.counterdict_all_emoji_if_match[c] += all_count[i]
            else:
                results.counterdict_all_emoji_if_match[c] = all_count[i]

        try:
            if tweet["lang"] in results.counterdict_lang.keys():
                results.counterdict_lang[tweet["lang"]] += 1
            else:
                results.counterdict_lang[tweet["lang"]] = 1
        except KeyError:
            continue

        # Count total numbers of emoji in tweet for each match subset
        for group in MATCHES:
            all_emoji, all_count = find_all_if(tweet["text"], MATCHES[group])
            if not all_emoji:
                continue
            for i, c in enumerate(all_emoji):
                attr_name = "counterdict_all_emoji_if_{}".format(group)
                results.add_to(c, all_count[i], attr_name)

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

            results_global.counterdict_lang = sum_dicts(
                results_global.counterdict_lang, results.counterdict_lang
            )
            results_global.counterdict_all_emoji = sum_dicts(
                results_global.counterdict_all_emoji, results.counterdict_all_emoji
            )
            results_global.counterdict_all_emoji_if_match = sum_dicts(
                results_global.counterdict_all_emoji_if_match,
                results.counterdict_all_emoji_if_match
            )

            results_global.counterdict_all_emoji_if_clockfaces = sum_dicts(
                results_global.counterdict_all_emoji_if_clockfaces,
                results.counterdict_all_emoji_if_clockfaces
            )
            results_global.counterdict_all_emoji_if_hourglasses = sum_dicts(
                results_global.counterdict_all_emoji_if_hourglasses,
                results.counterdict_all_emoji_if_hourglasses
            )
            results_global.counterdict_all_emoji_if_soon = sum_dicts(
                results_global.counterdict_all_emoji_if_soon,
                results.counterdict_all_emoji_if_soon
            )
            results_global.counterdict_all_emoji_if_watch = sum_dicts(
                results_global.counterdict_all_emoji_if_watch,
                results.counterdict_all_emoji_if_watch
            )
            results_global.counterdict_all_emoji_if_stopwatch = sum_dicts(
                results_global.counterdict_all_emoji_if_stopwatch,
                results.counterdict_all_emoji_if_stopwatch
            )
            results_global.counterdict_all_emoji_if_mantelpiece_clock = sum_dicts(
                results_global.counterdict_all_emoji_if_mantelpiece_clock,
                results.counterdict_all_emoji_if_mantelpiece_clock
            )
            results_global.counterdict_all_emoji_if_timer_clock = sum_dicts(
                results_global.counterdict_all_emoji_if_timer_clock,
                results.counterdict_all_emoji_if_timer_clock
            )
            results_global.counterdict_all_emoji_if_alarm_clock = sum_dicts(
                results_global.counterdict_all_emoji_if_alarm_clock,
                results.counterdict_all_emoji_if_alarm_clock
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
    print("Total Tweets w/ Match : {:d}".format(results_global.counter_total_match))

    return results_global


def save_results(results):
    """Save results to csv."""
    # Convert output to dataframe
    df_lang = pd.DataFrame(list(results.counterdict_lang.items()), columns=["Lang", "Count"])
    df_allemoji = pd.DataFrame(
        list(results.counterdict_all_emoji.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_match = pd.DataFrame(
        list(results.counterdict_all_emoji_if_match.items()), columns=["Emoji", "Count"]
    )

    df_allemoji_clockfaces = pd.DataFrame(
        list(results.counterdict_all_emoji_if_clockfaces.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_hourglasses = pd.DataFrame(
        list(results.counterdict_all_emoji_if_hourglasses.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_soon = pd.DataFrame(
        list(results.counterdict_all_emoji_if_soon.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_watch = pd.DataFrame(
        list(results.counterdict_all_emoji_if_watch.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_stopwatch = pd.DataFrame(
        list(results.counterdict_all_emoji_if_stopwatch.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_mantelpiece_clock = pd.DataFrame(
        list(results.counterdict_all_emoji_if_mantelpiece_clock.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_timer_clock = pd.DataFrame(
        list(results.counterdict_all_emoji_if_timer_clock.items()), columns=["Emoji", "Count"]
    )
    df_allemoji_alarm_clock = pd.DataFrame(
        list(results.counterdict_all_emoji_if_alarm_clock.items()), columns=["Emoji", "Count"]
    )

    # Export results as CSV files
    df_lang.to_csv("./langdata.csv", encoding="utf-8")
    df_allemoji.to_csv("./allemojidata.csv", encoding="utf-8")
    df_allemoji_match.to_csv("./allemojidatamatch.csv", encoding="utf-8")

    df_allemoji_clockfaces.to_csv("./allemojidatamatch_clockfaces.csv", encoding="utf-8")
    df_allemoji_hourglasses.to_csv("./allemojidatamatch_hourglasses.csv", encoding="utf-8")
    df_allemoji_soon.to_csv("./allemojidatamatch_soon.csv", encoding="utf-8")
    df_allemoji_watch.to_csv("./allemojidatamatch_watch.csv", encoding="utf-8")
    df_allemoji_stopwatch.to_csv("./allemojidatamatch_stopwatch.csv", encoding="utf-8")
    df_allemoji_mantelpiece_clock.to_csv("./allemojidatamatch_mantelpiece_clock.csv", encoding="utf-8")
    df_allemoji_timer_clock.to_csv("./allemojidatamatch_timer_clock.csv", encoding="utf-8")
    df_allemoji_alarm_clock.to_csv("./allemojidatamatch_alarm_clock.csv", encoding="utf-8")


if __name__ == "__main__":

    args = parse_cli_args()

    # Characters to match
    MATCHES = {
        "clockfaces": [
            # O'clock emoji
            EMOJI_UNICODE[":one_o\u2019clock:"],
            EMOJI_UNICODE[":two_o\u2019clock:"],
            EMOJI_UNICODE[":three_o\u2019clock:"],
            EMOJI_UNICODE[":four_o\u2019clock:"],
            EMOJI_UNICODE[":five_o\u2019clock:"],
            EMOJI_UNICODE[":six_o\u2019clock:"],
            EMOJI_UNICODE[":seven_o\u2019clock:"],
            EMOJI_UNICODE[":eight_o\u2019clock:"],
            EMOJI_UNICODE[":nine_o\u2019clock:"],
            EMOJI_UNICODE[":ten_o\u2019clock:"],
            EMOJI_UNICODE[":eleven_o\u2019clock:"],
            EMOJI_UNICODE[":twelve_o\u2019clock:"],
            # Half past the hour emoji
            EMOJI_UNICODE[":one-thirty:"],
            EMOJI_UNICODE[":two-thirty:"],
            EMOJI_UNICODE[":three-thirty:"],
            EMOJI_UNICODE[":four-thirty:"],
            EMOJI_UNICODE[":five-thirty:"],
            EMOJI_UNICODE[":six-thirty:"],
            EMOJI_UNICODE[":seven-thirty:"],
            EMOJI_UNICODE[":eight-thirty:"],
            EMOJI_UNICODE[":nine-thirty:"],
            EMOJI_UNICODE[":ten-thirty:"],
            EMOJI_UNICODE[":eleven-thirty:"],
            EMOJI_UNICODE[":twelve-thirty:"],
        ],
        # Other clock and time related emoji
        "hourglasses": [
            EMOJI_UNICODE[":hourglass_done:"],
            EMOJI_UNICODE[":hourglass_not_done:"],
        ],
        "soon": [EMOJI_UNICODE[":SOON_arrow:"]],
        "watch": [EMOJI_UNICODE[":watch:"]],
        "stopwatch": [EMOJI_UNICODE[":stopwatch:"]],
        "mantelpiece_clock": [EMOJI_UNICODE[":mantelpiece_clock:"]],
        "timer_clock": [EMOJI_UNICODE[":timer_clock:"]],
        "alarm_clock": [EMOJI_UNICODE[":alarm_clock:"]],
    }
    MATCHES_ALL = [emoji for lst in MATCHES.values() for emoji in lst]

    # Unpack and list all files
    if args.unpack:
        unpack_files(args.data_path)
    all_files = get_all_files(args.data_path, days=args.days, hours=args.hours)

    # Main search loop
    RESULTS_GLOBAL = run()
    save_results(RESULTS_GLOBAL)
