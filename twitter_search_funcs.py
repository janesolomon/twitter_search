#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
twittersearchfuncs.py
Functions for searching Twitter json

"""

import sys

__author__ = "Jane Solomon and Jeremy Smith"
__version__ = "1.0"


def progress(count, total, suffix=''):
    """Progress bar function"""
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('[%s] %#5.1f%s ...%s\r' % (bar, percents, '%', suffix))
    sys.stdout.flush()


def find_context(tweet, char):
    """Finds a character (char) in tweet and its nearest neighbors
    both character and word. Ignores spaces for the nearest character search.
    Leaves new line character in results to avoid associating character
    separated by a new line as a nearest neighbor. Only searches for
    the first instance of the character."""

    # Tweet split into a list of words
    tweet_word_list = tweet.split(' ')
    # Tweet rejoined into string thus removing all spaces
    tweet_clean = "".join(tweet_word_list)
    # Location of character
    loc = tweet_clean.find(char)
    # If character is not found (i.e. loc = -1) then return None
    if loc == -1:
        return None, None, None, None

    # Finds character before and after
    if len(tweet_clean) == 1:
        return None, None, None, None
    if loc == 0:
        char_before = None
        char_after = tweet_clean[loc + 1]
    elif loc == len(tweet_clean) - 1:
        char_before = tweet_clean[loc - 1]
        char_after = None
    else:
        char_before = tweet_clean[loc - 1]
        char_after = tweet_clean[loc + 1]

    # Word location of char
    wloc = next((tweet_word_list.index(w) for w in tweet_word_list if char in w), None)

    # Finds word before and after
    if len(tweet_word_list) == 1:
        word_before, word_after = None, None
    elif wloc == 0:
        word_before = None
        word_after = tweet_word_list[wloc + 1]
    elif wloc == len(tweet_word_list) - 1:
        word_before = tweet_word_list[wloc - 1]
        word_after = None
    else:
        word_before = tweet_word_list[wloc - 1]
        word_after = tweet_word_list[wloc + 1]

    return char_before, word_before, char_after, word_after
