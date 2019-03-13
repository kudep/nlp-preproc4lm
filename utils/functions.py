#!/usr/bin/python3
# -*- coding: utf-8 -*-


import re
import sys
import unicodedata
import collections
import hashlib

from nltk import word_tokenize
from ru_sent_tokenize import ru_sent_tokenize

# %%
tags_rm = [
    (re.compile(r"<ref name=.*?>"), " "),
    (re.compile(r"<div class=.*?>"), " "),
    (re.compile(r"</?\s?[^(math)(/math)(…)\.А-Яа-я0-9\"].{0,150}?>"), " "),
]

repeated_math_tag_rm = [(re.compile(r"(</?math>[^А-Яа-я]{0,150}?)</?math>"), " ")]

math_tag_rm = [(re.compile(r"</?math*?>"), " ")]


regex_norms = [
    (re.compile(r"&\w{0,};"), r""),  # &gt; less
    (re.compile(r"quot;"), r""),  # quot; less
    (re.compile(r"…"), r"..."),  # … -> ... (2026)
    (re.compile(r"\?{1,}![\?!]+"), r"?!"),  # ?!???!! -> ?!
    (re.compile(r"!{1,}\?[\?!]+"), r"?!"),  # ?!???!! -> ?!
    (re.compile(r"!{3,}"), r"!!"),  # !!!!!!!! -> !!
    (re.compile(r"\?{3,}"), r"??"),  # ???? -> ??
    (re.compile(r"\.{4,}"), r"..."),  # ....... -> ...
    (re.compile(r"[“”«»]"), r'"'),
    (re.compile(r"’"), r"'"),
    (re.compile(r"[`']{2,}"), r'"'),
    (re.compile(r"[‐‑‒–—―-]{1,}"), r"-"),  # (2010)(2011)(2012)(2013)(2014)(2015)(2016) -> - (2012)
    (re.compile(r"\s+"), r" "),
    (re.compile(r"­"), r""),  # remove u00ad
]

regex_trash_rm = [(re.compile(r"\(\s+\)"), r" "), (re.compile(r"\s+"), r" ")]


def apply_regex(in_line, regexs):
    line = in_line.strip()
    for regex, tgt in regexs:
        line = regex.sub(tgt, line)
    return line


def remove_tags(in_line):
    line = str(in_line)
    line = apply_regex(line, tags_rm)
    line = "<neli>".join(line.split("\n"))
    line = apply_regex(line, repeated_math_tag_rm)  # degree of nesting 3 math tags
    line = apply_regex(line, repeated_math_tag_rm)
    line = apply_regex(line, repeated_math_tag_rm)
    line = "\n".join(line.split("<neli>"))
    line = apply_regex(line, math_tag_rm)
    return line


combining_characters = dict.fromkeys([c for c in range(sys.maxunicode) if unicodedata.combining(chr(c))])


def rm_diacritic(in_line):
    sanitized_tokens = []
    for token in in_line.split():
        decomposed_token = unicodedata.normalize("NFD", token)
        sanitized_token = decomposed_token.translate(combining_characters)
        # Move bak reversed N with hat
        sanitized_token = "".join(ch_san if ch not in "йЙ" else ch for ch, ch_san in zip(token, sanitized_token))
        sanitized_token = unicodedata.normalize("NFC", sanitized_token)

        sanitized_tokens.append(sanitized_token)
    return " ".join(sanitized_tokens)


def split_punctuation(in_line):
    line = [" %s " % char if unicodedata.category(char).startswith("P") else char for char in in_line]
    return "".join(line)


url_pattern = re.compile(
    r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]\(\)\*{}\;:\'".,<>?\xab\xbb]))'
)

hashurl_pattern = re.compile(r"urlhash256\w*")
hash2url = {}


def change_url2urlhash(line):
    found_urls = url_pattern.findall(line)

    found_urls = [match[0] for match in found_urls]
    url2hash = [(url, "urlhash256" + hashlib.sha256(url.encode("utf-8")).hexdigest()) for url in found_urls]
    for url, url_hash in url2hash:
        line = line.replace(url, " " + url_hash + " ")
        hash2url[url_hash] = url

    return line


def change_urlhash2url(line):
    found_hashes = hashurl_pattern.findall(line)
    for url_hash in found_hashes:
        line = line.replace(url_hash, hash2url.get(url_hash, ""))
    return line


def doc2text(doc, skip_banned=True, add_title=True):
    if skip_banned or doc.get("banned", True):
        title = str(doc.get("title", "")).strip() if add_title else ""
        title = title if not (title) or title[-1] in ".?!" else title + "."
        cleaned_description = str(doc.get("cleaned_description", "")).strip()
        cleaned_description = (
            cleaned_description
            if not (cleaned_description) or cleaned_description[-1] in ".?!"
            else cleaned_description + "."
        )
        return (title + " " + cleaned_description).strip()
    return ""


def run_map(function, in_list):
    return list(map(function, in_list))


def tokenize(sentence):
    return " ".join(word_tokenize(sentence))


def drop_useless_chars(sentence, alphabet=None):
    if alphabet:
        return "".join(run_map(lambda ch: ch if ch in alphabet else "", sentence))
    else:
        return sentence


def sentense_out_of_vocab(sentence, vocab, trashhold=3, outof_partition=0.3):
    words = sentence.split()
    outof_count = len([None for w in words if w in vocab])
    return outof_count < trashhold and outof_count < len(words) * outof_partition


def docs2sentences(docs, alphabet, vocab):
    lines = run_map(doc2text, docs)
    # st1_len = len(lines)

    lines = run_map(lambda x: str(x).split("\n"), lines)
    lines = sum(lines, [])
    # st2_len = len(lines)

    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line]
    # st3_len = len(lines)

    sentences = []
    for line in lines:
        sub_lines = ru_sent_tokenize(line)
        sentences.extend(sub_lines)
    # st4_len = len(sentences)

    sentences_txt = "\n".join(sentences)
    sentences_txt = remove_tags(sentences_txt)
    sentences = sentences_txt.split("\n")
    # st5_len = len(sentences)

    sentences = [change_url2urlhash(line) for line in sentences]
    sentences = [apply_regex(line, regex_norms) for line in sentences]
    sentences = [rm_diacritic(line) for line in sentences if line]
    sentences = [apply_regex(line, regex_trash_rm) for line in sentences]
    sentences = run_map(tokenize, sentences)
    sentences = [change_urlhash2url(line) for line in sentences if line]
    # st6_len = len(sentences)

    sentences = [drop_useless_chars(line, alphabet) for line in sentences if line]
    # st7_len = len(sentences)

    sentences = [line.strip() for line in sentences]
    sentences = [line for line in sentences if len(line.split) > 3]
    sentences = [line for line in sentences if len(line) > 5]
    sentences = [line for line in sentences if sentense_out_of_vocab(line, vocab)]
    # st8_len = len(sentences)
    # print('st1_len = {}'.format(st1_len))
    # print('st2_len/st1_len = {}'.format(st2_len/st1_len))
    # print('st3_len/st1_len = {}'.format(st3_len/st1_len))
    # print('st4_len/st1_len = {}'.format(st4_len/st1_len))
    # print('st5_len/st1_len = {}'.format(st5_len/st1_len))
    # print('st6_len/st1_len = {}'.format(st6_len/st1_len))
    # print('st7_len/st1_len = {}'.format(st7_len/st1_len))
    # print('st8_len/st1_len = {}'.format(st8_len/st1_len))

    return sentences


def chunk_generator(items_list, chunk_size):
    for i in range(0, len(items_list), chunk_size):
        yield items_list[i : i + chunk_size]


def counters_merge(counters):
    while len(counters) > 1:
        count_pairs_gen = chunk_generator(counters, 2)
        counters = []
        for count_pair in count_pairs_gen:
            counters.append(sum(count_pair, collections.Counter()))
    return counters[-1] if counters else None
