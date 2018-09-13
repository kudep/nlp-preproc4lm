#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
#next level
import unicodedata
import sys
# from ufal.udpipe import Model, Pipeline

import pandas as pd

from rusenttokenize import ru_sent_tokenize
from nltk import word_tokenize

url_pattern = r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]{};:\'".,<>?\xab\xbb]))'
user_pattern = r'((RT ){0,1}@[\w\d]*?:{0,1}) '
tag_pattern = r'#[\S]*'
num_pattern = r'[\d]{1,}'


def skip_empty(in_line):
    line = str(in_line)
    line =  line.strip()
    if line:
        return line

def get_rec_info(in_line):
    line = in_line.strip()
    found_urls = re.findall(url_pattern, line)
    found_urls = [match[0] for match in found_urls]
    found_users = re.findall(user_pattern, line)
    found_users = [match[0] for match in found_users]
    found_tags = re.findall(tag_pattern, line)
    found_nums = re.findall(num_pattern, line)
    recovery_info = {'found_urls':found_urls,
                   'found_users':found_users,
                   'found_tags':found_tags,
                   'found_nums':found_nums,
                    }
    return recovery_info

def spec_tok_add(in_line):
    line = in_line.strip()
    line = re.sub(url_pattern, ' <URL> ', line) # swap urls
    line = re.sub(user_pattern, ' <USR> ', line) # swap urls
    line = re.sub(tag_pattern, ' <HASHTAG> ', line) # swap urls
    line = re.sub(num_pattern, ' <NUM> ', line) # swap urls
    line = line.strip()
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

def _gen(elms):
    for e in elms:
        yield e

def recovery(in_row):
    in_line, rec = in_row['norm_text'], in_row['rec']
    line = in_line.strip(' ')
    tokens = line.split(' ')
    urls_gen = _gen(rec['found_urls'])
    users_gen = _gen(rec['found_users'])
    tags_gen = _gen(rec['found_tags'])
    nums_gen = _gen(rec['found_nums'])
    urls_sample = next(urls_gen, None)
    users_sample = next(users_gen, None)
    tags_sample = next(tags_gen, None)
    nums_sample = next(nums_gen, None)
    out_tokens = []
    for tok in tokens:
        if urls_sample and tok=='<URL>':
            out_tokens.append(urls_sample)
            urls_sample = next(urls_gen, None)
        elif users_sample and tok=='<USR>':
            out_tokens.append(users_sample.split()[-1])
            users_sample = next(users_gen, None)
        elif tags_sample and tok=='<HASHTAG>':
            out_tokens.append(tags_sample)
            tags_sample = next(tags_gen, None)
        elif nums_sample and tok=='<NUM>':
            out_tokens.append(nums_sample)
            nums_sample = next(nums_gen, None)
        else:
            out_tokens.append(tok)
    line = ' '.join(out_tokens)
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

def lower_case(in_line):
    return in_line.lower()

def split_lines(lines, separator):
    out_lines = []
    def splitListToRows(line):
        sentences = line.split(separator)
        for s in sentences:
            out_lines.append(s)
    map(func.lower_case, lines)
    return out_lines
