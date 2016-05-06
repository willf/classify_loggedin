# -*- coding: utf-8 -*-

import collections
import json
import os
import re
import sys
import warnings

from bs4 import BeautifulSoup


"""Tokenizer/Analyzer for a directory of JSON files.

json_directory_tokenizer.py will tokenize/analyze a directory of
JSON files.

Its default behavior is to lowercase each string, remove non-alphanums,
and use all fields.
"""


def strip_html(s):
    """Strip the HTML from string
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ''.join(BeautifulSoup(s, 'lxml').findAll(text=True))


def strip_punct(s):
    "Strips the punctuation, more or less, from a string."
    pattern = re.compile('[\W_]+')
    return pattern.sub(' ', s).strip()


class JsonTokenizerConfig:
    """A simple configuration object to pass to the tokenizer
    functions.
    tokenizer: splits a string into tokens. Default splits on spaces.
    prepreprocessor: prepreprocesses the string. default lowercases,
    strips HTML, strips punctuation.
    fields: which fields from a JSON object should be kept
    """

    def __init__(self,
                 tokenizer=lambda s: s.split(' '),
                 prepreprocessor=lambda s: strip_punct(strip_html(s.lower())),
                 fields=None):
        self.tokenizer = tokenizer
        self.prepreprocessor = prepreprocessor
        self.fields = fields


def json_tokenize(obj, json_tokenizer_config):
    """json_tokenize tokenizes a dict (JSON object) according to a config.

     It yields a stream of tokens of the form FIELD:WORD from the
     values for each field in the dict, from the tokens in the value of the
     fields.
    """
    if json_tokenizer_config.fields is None:
        fields = obj.keys()
    else:
        fields = json_tokenizer_config.fields
    sents = [str(obj.get(field, '')) for field in fields]
    tokenized = [json_tokenizer_config.tokenizer(
        json_tokenizer_config.prepreprocessor(sent)) for sent in sents]
    z = zip(fields, tokenized)
    for pair in z:
        (field, tokens) = pair
        for token in tokens:
            yield "%s:%s" % (field, token)


def json_directory(directory):
    """json_directory yields a list of dicts (JSON objects) from a directory

    Yields: (object, identifier, None) in case of no error
    Yields: (None, None, ValueError) in case of error

    """
    for (dirpath, dirnames, filenames) in os.walk(directory):
        for filename in filenames:
            pathname = dirpath + '/' + filename
            with open(pathname) as data_file:
                try:
                    obj = json.load(data_file)
                    yield obj, obj.get('identifier', ''), None
                except:
                    yield None, None, ValueError()


def tokenize_directory(classification, directory, json_tokenizer_config):
    """tokenize_directory tokenizes the files in the directory

    Yields: (classification, identifier, tokens) for each valid file
    """
    for obj, identifier, r in json_directory(directory):
        if obj:
            yield (classification, identifier,
                   list(json_tokenize(obj, json_tokenizer_config)))


def token_counts(directory, json_tokenizer_config):
    """token_counts returns a counter with all of the token counts
    """
    c = collections.Counter()
    for obj, identifier, r in json_directory(directory):
        if obj:
            for token in json_tokenize(obj, json_tokenizer_config):
                c[token] += 1
    return c


def field_counts(directory):
    c = collections.Counter()
    for obj, ident, err in json_directory(directory):
        if obj:
            for k in obj.keys():
                c[k] += 1
    return c
