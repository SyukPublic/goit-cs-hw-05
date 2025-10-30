# -*- coding: utf-8 -*-

"""
HomeWork Task 2
"""

import argparse
import string

from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, ItemsView

import requests
import matplotlib.pyplot as plt


def get_text(url: str) -> Optional[str]:
    """
    Downloading text from the specified URL.

    :param url: URL for downloading
    :return: The downloaded text, or None if the download failed
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def remove_punctuation(text: str) -> str:
    """
    Removal of punctuation marks.

    :param text: Original text
    :return: Text without punctuation marks
    """
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word: str) -> tuple[str, int]:
    """
    Map function.

    :param word: The word
    :return: Tuple key-value
    """
    return word, 1


def shuffle_function(mapped_values: list[tuple[str, int]]) -> ItemsView[str, list[int]]:
    """Shuffle function.

    :param mapped_values: List of key-value pairs
    :return:
    """
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values) -> tuple[str, int]:
    """
    Reduce function.

    :param key_values: Key-values pair
    :return: Aggregated result for key
    """
    key, values = key_values
    return key, sum(values)


def map_reduce(text: str, search_words: Optional[list[str]] = None) -> dict[str, int]:
    """
    MapReduce for counting words in a text.

    :param text: Text
    :param search_words: List of words to search for
    :return: Dictionary with the count results
    """

    # Removal of punctuation marks
    text = remove_punctuation(text)
    words = text.split()

    # If a list of search words is provided, include only those words.
    if search_words:
        words = [word for word in words if word in search_words]

    # Parallel Mapping
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # Shuffle
    shuffled_values = shuffle_function(mapped_values)

    # Parallel Reduction
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


def visualize_top_words(result: dict[str, int], top_number: int = 10) -> None:
    """
    Visualize the top N most common words in the text.

    :param result: Dictionary with the words count results
    :param top_number: Number of the most common words
    """

    top_words = Counter(result).most_common(top_number)
    words, counts = zip(*top_words)

    plt.figure(figsize=(10, 6))
    plt.barh(words, counts, color="skyblue")
    plt.gca().invert_yaxis()
    plt.title(f"Top {top_number} Most Frequent Words")
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.tight_layout()
    plt.show()


def cli() -> None:
    try:
        parser = argparse.ArgumentParser(
            description="MapReduce words counter", epilog="Good bye!"
        )
        parser.add_argument(
            "-l",
            "--url",
            type=str,
            default="https://gutenberg.net.au/ebooks01/0100021.txt",
            help="URL for downloading the text (Default: https://gutenberg.net.au/ebooks01/0100021.txt)",
        )

        args = parser.parse_args()

        text = get_text(args.url)
        if text:
            result = map_reduce(text)
            visualize_top_words(result)
        else:
            print("Error: Failed to download the text from the specified URL")
    except Exception as e:
        print(e)

    exit(0)
