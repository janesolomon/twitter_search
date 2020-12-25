#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions reading twitter data dumps
"""
import os
import json
import bz2
import tarfile


def read_zip(filename):
    """Reads tweet zip file from https://archive.org/details/twitterstream.

    Args:
        filename (str)

    Yields:
        dict
    """
    # Decompress and decode .bz2 file
    fbz = bz2.BZ2File(filename, "rb")
    try:
        fdec = fbz.read()
    except IOError:
        return None
    finally:
        fbz.close()
    fdecutf = fdec.decode("utf-8")

    # Loop through each line in file
    for tweet_str in fdecutf.split("\n"):
        try:
            tweet = json.loads(tweet_str)
        except ValueError:
            continue
        if "delete" in tweet.keys():
            continue
        yield tweet


def unpack_files(data_path):
    """Unpacks tar files.

    Args:
        filename (str)
    """
    for filename in os.listdir(data_path):
        if filename.endswith(".tar"):
            with tarfile.open(os.path.join(data_path, filename), "r:") as tar:
                tar.extractall(path=data_path)


def get_all_files(data_path, days=31, hours=24):
    """Produces a flattened list of all zip files in the tweet data
    hierarchy structured by `month/day/hour/file.bz2`.

    Returns:
        List[str]
    """
    all_files = []

    for day in range(days):
        day_str = "{:02d}".format(day + 1)
        for hour in range(hours):
            hour_str = "{:02d}".format(hour)
            file_path = os.path.join(data_path, day_str, hour_str)
            files = os.listdir(file_path)

            for i, file in enumerate(files):
                files[i] = os.path.join(file_path, file)

            all_files.extend(files)

    return all_files
