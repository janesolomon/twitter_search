#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Functions for searching Twitter json

"""
from collections import Counter

from twitter_search.unicode_codes import EMOJI_UNICODE_SET

__all__ = ["find_context", "find_all", "find_all_if", "smoothed_relative_freq", "sum_dicts"]


def _list_clean(tweet):
    """Splits the tweet into words based on spaces and returns this list
    along with a concatenated string without spaces.

    Args:
        tweet (str): Tweet text

    Returns:
        tuple
    """
    # Tweet split into a list of words
    tweet_word_list = tweet.split(" ")
    # Tweet rejoined into string thus removing all spaces
    tweet_clean = "".join(tweet_word_list)

    return tweet_word_list, tweet_clean


def find_context(tweet, char):
    """Finds a character (char) in tweet and its nearest neighbors
    both character and word. Ignores spaces for the nearest character search.
    Leaves new line character in results to avoid associating character
    separated by a new line as a nearest neighbor. Only searches for
    the first instance of the character.

    Args:
        tweet (str): Tweet text
        char (str): Character to search for

    Returns:
        tuple
    """

    # Clean tweet
    tweet_word_list, tweet_clean = _list_clean(tweet)
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
    and their counts.

    Args:
        tweet (str): Tweet text

    Returns:
        tuple
    """

    # Clean tweet
    tweet_word_list, tweet_clean = _list_clean(tweet)

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


def find_all_if(tweet, chars):
    """Find all occurrences of emoji in a tweet when any of `chars` are
    present in the tweet. Includes counts of emoji in the `chars` list.
    Returns a list of those emoji and their counts.

    Args:
        tweet (str): Tweet text
        chars (List[str]): List of characters to search for

    Returns:
        tuple
    """

    # Clean tweet
    tweet_word_list, tweet_clean = _list_clean(tweet)

    # Tweet characters split into list
    tweet_clean_charlist = list(tweet_clean)

    if not any(char in chars for char in tweet_clean_charlist):
        return None, None

    # Find matches using intersection of emoji list and tweet
    matches = list(EMOJI_UNICODE_SET.intersection(tweet_clean_charlist))

    # Count number of each emoji
    counts = [tweet.count(c) for c in matches]

    return matches, counts


def smoothed_relative_freq(n_focus, n_ref, size_focus, size_ref, N=1):
    """Simple maths method for finding relative frequency of a word in the
    focus corpus compared to the reference corpus. Frequencies are
    calculated per million and N is the smoothing parameter (default N = 1).

    Args:
        n_focus (int): Number of target words in focus corpus
        n_ref (int): Number of target words in reference corpus
        size_focus (int): Size of focus corpus
        size_ref (int): Size of reference corpus
        N (int, optional): Smoothing parameter

    Returns:
        float
    """

    f_focus = n_focus * 1.0e6 / size_focus
    f_ref = n_ref * 1.0e6 / size_ref

    return (f_focus + N) / (f_ref + N)


def sum_dicts(a, b):
    """Merge dictionaries, summing their values.

    For example:
        a = {"x": 1, "y": 1}
        b = {"x": 1, "z": 1}
        returns {"x": 2, "y": 1, "z": 1}

    Args:
        a (dict)
        b (dict)

    Returns:
        dict
    """
    return Counter(a) + Counter(b)
