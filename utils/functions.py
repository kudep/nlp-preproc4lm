#!/usr/bin/python3
# -*- coding: utf-8 -*-
import hashlib
from ru_sent_tokenize import ru_sent_tokenize
import re

import unicodedata
import sys
# %%
tags_rm = [
    (re.compile(r'<ref name=.*?>'), ' '),
    (re.compile(r'<div class=.*?>'), ' '),
    (re.compile(r'</?\s?[^(math)(/math)(…)\.А-Яа-я0-9\"].{0,150}?>'), ' '),
]

repeated_math_tag_rm = [
    (re.compile(r'(</?math>[^А-Яа-я]{0,150}?)</?math>'), ' '),
]

math_tag_rm = [
    (re.compile(r'</?math*?>'), ' '),
]


regex_norms = [
    (re.compile(r'&\w{0,};'), r''),  # &gt; less
    (re.compile(r'quot;'), r''),  # quot; less
    (re.compile(r'…'), r'...'),  # … -> ... (2026)
    (re.compile(r'\?{1,}![\?!]+'), r'?!'),  # ?!???!! -> ?!
    (re.compile(r'!{1,}\?[\?!]+'), r'?!'),  # ?!???!! -> ?!
    (re.compile(r'!{3,}'), r'!!'),  # !!!!!!!! -> !!
    (re.compile(r'\?{3,}'), r'??'),  # ???? -> ??
    (re.compile(r'\.{4,}'), r'...'),  # ....... -> ...
    (re.compile(r'[“”«»]'), r'"'),
    (re.compile(r"’"), r"'"),
    (re.compile(r"[`']{2,}"), r'"'),
    (re.compile(r'[‐‑‒–—―-]{1,}'), r'-'),  # (2010)(2011)(2012)(2013)(2014)(2015)(2016) -> - (2012)
    (re.compile(r'\s+'), r' '),
    (re.compile(r'­'), r''),  # remove u00ad
]

regex_trash_rm = [
    (re.compile(r'\(\s+\)'), r' '),
    (re.compile(r'\s+'), r' '),
]


def apply_regex(in_line, regexs):
    line = in_line.strip()
    for regex, tgt in regexs:
        line = regex.sub(tgt, line)
    return line


def remove_tags(in_line):
    line = str(in_line)
    line = apply_regex(line, tags_rm)
    line = '<neli>'.join(line.split('\n'))
    line = apply_regex(line, repeated_math_tag_rm)  # degree of nesting 3 math tags
    line = apply_regex(line, repeated_math_tag_rm)
    line = apply_regex(line, repeated_math_tag_rm)
    line = '\n'.join(line.split('<neli>'))
    line = apply_regex(line, math_tag_rm)
    return line


combining_characters = dict.fromkeys([c for c in range(sys.maxunicode)
                                      if unicodedata.combining(chr(c))])


def rm_diacritic(in_line):
    sanitized_tokens = []
    for token in in_line.split():
        decomposed_token = unicodedata.normalize('NFD', token)
        sanitized_token = decomposed_token.translate(combining_characters)
        # Move bak reversed N with hat
        sanitized_token = ''.join(ch_san if ch not in 'йЙ' else ch for ch, ch_san in zip(token, sanitized_token))
        sanitized_token = unicodedata.normalize('NFC', sanitized_token)

        sanitized_tokens.append(sanitized_token)
    return ' '.join(sanitized_tokens)


def split_punctuation(in_line):
    line = [' %s ' % char if unicodedata.category(char).startswith('P') else char for char in in_line]
    return "".join(line)


url_pattern = re.compile(
    r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]\(\)\*{}\;:\'".,<>?\xab\xbb]))')

hashurl_pattern = re.compile(r'urlhash256\w*')
hash2url = {}


def change_url2urlhash(line):
    found_urls = url_pattern.findall(line)

    found_urls = [match[0] for match in found_urls]
    url2hash = [(url, 'urlhash256' + hashlib.sha256(url.encode('utf-8')).hexdigest()) for url in found_urls]
    for url, url_hash in url2hash:
        line = line.replace(url, ' ' + url_hash + ' ')
        hash2url[url_hash] = url

    return line


def change_urlhash2url(line):
    found_hashes = hashurl_pattern.findall(line)
    for url_hash in found_hashes:
        line = line.replace(url_hash, hash2url.get(url_hash, ''))
    return line


def doc2sentences(doc):
    lines = doc.get('text', '')
    lines = lines.split('\n')
    lines = [line for line in lines if line]
    sentences = []
    for line in lines:
        sub_lines = ru_sent_tokenize(line)
        sentences.extend(sub_lines)

    sentences_txt = '\n'.join(sentences)
    sentences_txt = remove_tags(sentences_txt)
    sentences = sentences_txt.split('\n')
    sentences = [change_url2urlhash(line) for line in sentences]
    sentences = [apply_regex(line, regex_norms) for line in sentences]
    sentences = [rm_diacritic(line) for line in sentences if line]
    # sentences = [split_punctuation(line) for line in sentences if line]
    sentences = [apply_regex(line, regex_trash_rm) for line in sentences]
    sentences = [change_urlhash2url(line) for line in sentences if line]
    sentences = [line for line in sentences if len(line.strip()) > 1]
    sentences = sentences[1:]
    if len(sentences) < 3:
        return []
    return sentences
