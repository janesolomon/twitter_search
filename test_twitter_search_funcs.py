#!/usr/bin/env python
"""
Unit tests for twitter_search_funcs.py
"""
from __future__ import print_function, unicode_literals

import unittest
from twitter_search_funcs import find_context as fc


class TestIt(unittest.TestCase):

    def test_find_context_not_found(self):
        # Arrange
        tweet = "has no eks character"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_unit_length(self):
        # Arrange
        tweet = "x"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_words_before_and_after(self):
        # Arrange
        tweet = "before x after"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertEqual(char_before, "e")
        self.assertEqual(word_before, "before")
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "after")

    def test_find_context_at_start_of_tweet(self):
        # Arrange
        tweet = "x at start of tweet"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "a")
        self.assertEqual(word_after, "at")

    def test_find_context_at_end_of_tweet(self):
        # Arrange
        tweet = "tweet ending in x"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertEqual(char_before, "n")
        self.assertEqual(word_before, "in")
        self.assertIsNone(char_after)
        self.assertIsNone(word_after)

    def test_find_context_repeated_target(self):
        # Arrange
        tweet = "xx"
        char = "x"

        # Act
        char_before, word_before, char_after, word_after = fc(tweet, char)

        # Assert
        self.assertIsNone(char_before)
        self.assertIsNone(word_before)
        self.assertEqual(char_after, "x")
        self.assertIsNone(word_after)


if __name__ == '__main__':
    unittest.main()

# End of file
