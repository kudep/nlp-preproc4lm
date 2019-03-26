#!/usr/bin/python3
# -*- coding: utf-8 -*-
import hashlib
from nltk import word_tokenize
from mosestokenizer import MosesDetokenizer

import re

import json
import bz2


def bz2file2utters(in_file, lang="en"):
    lines = [json.loads(line) for line in bz2.open(str(in_file)).readlines()]
    lines = [str(line.get("text", "")).strip() for line in lines if line.get("lang") == lang]
    lines = [line for line in lines if line]
    return lines


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
    (re.compile(r"­"), r""),  # remove u00ad
    (re.compile(r"­[\n\t\r]"), r" "),
    (re.compile(r"\s+"), r" "),
]

regex_trash_rm = [(re.compile(r"\(\s+\)"), r" "), (re.compile(r"\s+"), r" ")]


def apply_regex(in_line, regexs):
    line = in_line.strip()
    for regex, tgt in regexs:
        line = regex.sub(tgt, line)
    return line


detokenizer = MosesDetokenizer()


def clean_punctuation(lines):
    lines = [detokenizer(word_tokenize(ut)) for ut in lines]
    return lines


url_pattern = re.compile(
    r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]\(\)\*{}\;:\'".,<>?\xab\xbb]))'
)
hashurl_pattern = re.compile(r"urlhash256\w*")
hash2url = {}


usr_pattern = re.compile(r"(((RT ){0,1}@[\w\d]*\s*:{0,1}))")
hashusr_pattern = re.compile(r"usrhash256\w*")
hash2usr = {}


tag_pattern = re.compile(r"((#\S*))")
hashtag_pattern = re.compile(r"taghash256\w*")
hash2tag = {}


def change_obj2objhash(line, objs_pattern, hash2obj, spec_offset, flag=None):
    found_objs = objs_pattern.findall(line)

    if flag: print(found_objs)
    found_objs = [match[0] for match in found_objs]
    obj2hash = [(obj, spec_offset + hashlib.sha256(obj.encode("utf-8")).hexdigest()) for obj in found_objs]
    for obj, obj_hash in obj2hash:
        line = line.replace(obj, " " + obj_hash + " ")
        hash2obj[obj_hash] = obj

    return line


def change_objhash2obj(line, hash_objs_pattern, hash2obj):
    found_hashes = hash_objs_pattern.findall(line)
    for obj_hash in found_hashes:
        line = line.replace(obj_hash, hash2obj.get(obj_hash, ""))
    return line


def utters2utters(utters):
    # print(utters)
    lines = [" ".join((str(line).split())).strip() for line in utters]
    lines = [line for line in lines if line]
    # print(lines)

    lines = [change_obj2objhash(line, url_pattern, hash2url, spec_offset="urlhash256") for line in lines]
    lines = [change_obj2objhash(line, usr_pattern, hash2usr, spec_offset="usrhash256") for line in lines]
    lines = [change_obj2objhash(line, tag_pattern, hash2tag, spec_offset="taghash256") for line in lines]
    # print(lines)
    # print(hash2url)
    # print(hash2usr)
    # print(hash2tag)

    lines = [apply_regex(line, regex_norms) for line in lines]
    # lines = [split_punctuation(line) for line in lines if line]
    lines = [apply_regex(line, regex_trash_rm) for line in lines]
    lines = clean_punctuation(lines)
    # print(lines)

    lines = [" ".join((str(line).split())).strip() for line in lines]
    lines = [line for line in lines if line]
    lines = [change_objhash2obj(line, hashurl_pattern, hash2url) for line in lines]
    lines = [change_objhash2obj(line, hashusr_pattern, hash2usr) for line in lines]
    lines = [change_objhash2obj(line, hashtag_pattern, hash2tag) for line in lines]
    # print(lines)

    lines = [line for line in lines if len(line.strip()) > 1]
    lines = [str(line).strip() for line in lines]
    lines = list(set(lines))
    return lines
