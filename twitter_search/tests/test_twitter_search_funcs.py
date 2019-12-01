#!/usr/bin/env python
"""
Unit tests for twitter_search_funcs.py
"""
from __future__ import print_function, unicode_literals

import unittest
from twitter_search_funcs import find_context, find_all


class TestFindContext(unittest.TestCase):

    def test_find_context_not_found(self):

        tweet = "has no eks character"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_unit_length(self):

        tweet = "x"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_words_before_and_after(self):

        tweet = "before x after"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertEqual(char_before, "e")
        self.assertEqual(word_before, "before")
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "after")

    def test_find_context_at_start_of_tweet(self):

        tweet = "x at start of tweet"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "at")

    def test_find_context_at_end_of_tweet(self):

        tweet = "tweet ending in x"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertEqual(char_before, "n")
        self.assertEqual(word_before, "in")
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_repeated_target(self):

        tweet = "xx"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "x")
        self.assertIsNone(word_after)


class TestFindAll(unittest.TestCase):

    def test_find_all_not_found(self):

        tweet = "no emoji in text"
        matches, counts = find_all(tweet)

        self.assertIsNone(matches)
        self.assertIsNone(counts)

    def test_find_all_single(self):

        tweet = "one emoji in text 😂"
        matches, counts = find_all(tweet)

        self.assertEqual(matches, ["😂"])
        self.assertEqual(counts, [1])

    def test_find_all_multiple(self):

        tweet = "multiple emoji 😍 in text 😂😂"
        matches, counts = find_all(tweet)

        self.assertEqual(matches, ["😍", "😂"])
        self.assertEqual(counts, [1, 2])


if __name__ == '__main__':
    unittest.main()
