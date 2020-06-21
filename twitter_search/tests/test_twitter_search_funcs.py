#!/usr/bin/env python
"""
Unit tests for twitter_search_funcs.py
"""
from __future__ import print_function, unicode_literals

import unittest
from twitter_search import find_context, find_all, smoothed_relative_freq


class TestFindContext(unittest.TestCase):
    """Test find context function
    """
    def test_find_context_not_found(self):
        """Test for no char found
        """
        tweet = "has no eks character"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_unit_length(self):
        """Test for single char length tweet
        """
        tweet = "x"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_words_before_and_after(self):
        """Test for before and after char
        """
        tweet = "before x after"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertEqual(char_before, "e")
        self.assertEqual(word_before, "before")
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "after")

    def test_find_context_at_start_of_tweet(self):
        """Test for char at start
        """
        tweet = "x at start of tweet"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "at")

    def test_find_context_at_end_of_tweet(self):
        """Test for char at end
        """
        tweet = "tweet ending in x"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertEqual(char_before, "n")
        self.assertEqual(word_before, "in")
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_repeated_target(self):
        """Test for repeated char
        """
        tweet = "xx"
        char = "x"
        char_before, word_before, char_after, word_after = find_context(tweet, char)

        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "x")
        self.assertIsNone(word_after)


class TestFindAll(unittest.TestCase):
    """Test find all function
    """
    def test_find_all_not_found(self):
        """Test for no emoji found
        """
        tweet = "no emoji in text"
        matches, counts = find_all(tweet)

        self.assertIsNone(matches)
        self.assertIsNone(counts)

    def test_find_all_single(self):
        """Test for single emoji found
        """
        tweet = "one emoji in text 😂"
        matches, counts = find_all(tweet)

        self.assertEqual(matches, ["😂"])
        self.assertEqual(counts, [1])

    def test_find_all_multiple(self):
        """Test for multiple emoji found
        """
        tweet = "multiple emoji 😍 in text 😂😂"
        matches, counts = find_all(tweet)

        self.assertCountEqual(matches, ["😍", "😂"])
        self.assertCountEqual(counts, [1, 2])


class TestSmoothedRelativeFreq(unittest.TestCase):
    """Test smoothed relative frequency function
    """
    def test_smoothed_relative_freq(self):
        """Test smoothed relative frequency function
        """
        rel_freq_1 = smoothed_relative_freq(12, 321, 1e6, 1e7, N=1)
        rel_freq_10 = smoothed_relative_freq(12, 321, 1e6, 1e7, N=10)
        rel_freq_100 = smoothed_relative_freq(12, 321, 1e6, 1e7, N=100)

        self.assertAlmostEqual(rel_freq_1, 0.3927492447)
        self.assertAlmostEqual(rel_freq_10, 0.5225653207)
        self.assertAlmostEqual(rel_freq_100, 0.8478425435)


if __name__ == '__main__':
    unittest.main()
