#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
twittersearchfuncs.py
Functions for searching Twitter json

"""

from unicode_codes import EMOJI_UNICODE_SET

__author__ = "Jane Solomon and Jeremy Smith"
__version__ = "1.0"


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


def find_all(tweet):
    """Finds all occurrences of emoji in a tweet. Returns a list of those
    and their counts."""

    # Tweet split into a list of words
    tweet_word_list = tweet.split(' ')
    # Tweet rejoined into string thus removing all spaces
    tweet_clean = "".join(tweet_word_list)
    # Tweet characters split into list
    tweet_clean_charlist = list(tweet_clean)

    # Find matches using intersection of emoji list and tweet
    matches = list(EMOJI_UNICODE_SET.intersection(tweet_clean_charlist))

    # Check if there are any matches and return if not
    if len(matches) == 0:
        return None, None

    # Count number of each emoji
    counts = [tweet.count(c) for c in matches]

    return matches, counts


def smoothed_relative_freq(n_focus, n_ref, size_focus, size_ref, N=1):
    """Simple maths method for finding relative frequency of a word in the
    f_focus corpus compared to the reference corpus. Frequencies are
    calculated per million and N is the smoothing paramter (default N = 1)."""

    f_focus = n_focus * 1.e6 / size_focus
    f_ref = n_ref * 1.e6 / size_ref

    return (f_focus + N) / (f_ref + N)
