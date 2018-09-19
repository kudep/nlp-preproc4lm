#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
#next level
import unicodedata
import sys
# from ufal.udpipe import Model, Pipeline

import pandas as pd
import collections

from rusenttokenize import ru_sent_tokenize
from nltk import word_tokenize


SHORT_LEN = 6
EMPTY_LINE = '<empty_line>'
def skip_short_line(in_lines):
    pre_line_tokens, line_tokens = (str(line).strip().split() for line in in_lines)
    if not(line_tokens):
        return EMPTY_LINE
    if not (len(line_tokens) < SHORT_LEN and len(pre_line_tokens) < SHORT_LEN):
        return ' '.join(line_tokens)

MAX_DOT_PAIR = 4
def skip_contents_line(in_line):
    line = str(in_line)
    if len(re.findall(r'\. \. ', line)) < MAX_DOT_PAIR:
        return line

def skip_spaced_line(in_line):
    line = str(in_line)
    max_spaces_n = len(line)//3
    spaces_n = len(line.strip().split())
    if spaces_n < max_spaces_n:
        return line

def skip_not_char_line(in_line):
    line = str(in_line)
    max_char_n = len(line)//2
    char_n = len(re.findall(r'[A-z]', line))
    if char_n > max_char_n:
        return line

def remove_end_dash(in_line):
    line = str(in_line)
    line = re.sub(r'-\s{,}$','',line)
    return line

def normalization1(in_line):
    line = in_line.strip()
    #----------------
    line = re.sub(r'&\w{0,};', '', line) # &gt; less
    line = re.sub(r'quot;', '', line) # quot; less
    #----------------
    line = re.sub(r'…', r'...', line) # … -> ... (2026)
    line = re.sub(r'\?{1,}![\?!]+', r'?!', line) # ?!???!! -> ?!
    line = re.sub(r'!{1,}\?[\?!]+', r'?!', line) # ?!???!! -> ?!
    line = re.sub(r'!{3,}', r'!!', line) # !!!!!!!! -> !!
    line = re.sub(r'\?{3,}', r'??', line) # ???? -> ??
    line = re.sub(r'\.{4,}', r'...', line) # ....... -> ...
    line = re.sub(r'[“”«»]', r'"', line) #
    line = re.sub(r"’", r"'", line) #
    line = re.sub(r"[`']{2,}", r'"', line) #
    line = re.sub(r'[‐‑‒–—―-]{1,}', r'-', line) # (2010)(2011)(2012)(2013)(2014)(2015)(2016) -> - (2012)
    #----------------
    line = re.sub(r"\s+", r" ", line)
    line = re.sub(r'­', r'', line) # remove u00ad
    return line


def get_udpipe_sent_and_tok(udpipeline):

    def udpipe_sent_and_tok(in_line):
        line = udpipeline.process(in_line).split('\n')
        return line
    return udpipe_sent_and_tok


def nltk_sent_and_tok(in_line):
    sentences = ru_sent_tokenize(in_line)
    line = [' '.join(word_tokenize(sentence)) for sentence in sentences]
    return line

combining_characters = dict.fromkeys([c for c in range(sys.maxunicode)
                                if unicodedata.combining(chr(c))])
def normalization2(in_line):
    sanitized_sentences=[]
    for sentence in in_line:
        tokens = sentence.split()
        sanitized_tokens = []
        for token in tokens:
            decomposed_token = unicodedata.normalize('NFD', token)
            sanitized_token = decomposed_token.translate(combining_characters)
            # Move bak reversed N with hat
            sanitized_token = ''.join(ch_san if ch not in 'йЙ' else ch for ch, ch_san in zip(token, sanitized_token))
            sanitized_token = unicodedata.normalize('NFC', sanitized_token)

            sanitized_tokens.append(sanitized_token)
        if sanitized_tokens:
            sanitized_sentences.append(' '.join(sanitized_tokens))
    return ' \n '.join(sanitized_sentences)

def split_lines(lines, separator):
    out_lines = []
    def splitListToRows(line):
        sentences = line.split(separator)
        for s in sentences:
            out_lines.append(s)
    list(map(splitListToRows, lines))
    return out_lines

def chunk_generator(items_list, chunk_size):
    for i in range(0, len(items_list), chunk_size):
        yield items_list[i:i + chunk_size]

def counters_merge(counters):
    while len(counters) > 1:
        count_pairs_gen = chunk_generator(counters, 2)
        counters = []
        for count_pair in count_pairs_gen:
            counters.append(sum(count_pair, collections.Counter()))
    return counters[-1] if counters else None
