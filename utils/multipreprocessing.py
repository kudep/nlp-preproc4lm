#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
import tqdm
import json
import bz2
from multiprocessing import Pool
#next level
import unicodedata
import sys
from ufal.udpipe import Model, Pipeline


def get_json_bz2_files(dir_name):
    json_bz2_files = []
    pattern = re.compile(r'.*\.json\.bz2')
    for step in os.walk(dir_name):
         json_bz2_files.extend([os.path.join(step[0],file) for file in step[-1] if pattern.findall(file)])
    return json_bz2_files


def initializer(modelfile):
    global modelfile4udpipe
    modelfile4udpipe = modelfile

def preproc_step0(data):
    lines = []
    for line in data:
        if not line : continue
        line = json.loads(line)
        if not ('text' in line and 'lang' in line and 'ru' in line['lang']) : continue
        line = line['text']
        line = line.strip()
        lines.append(line)
    return lines

#########################
#Save first version
#########################
# def preproc_step1(data):
#     lines = []
#     for line in data:
#         line = line.strip()
#         #----------------
#         #----------------
#         line = re.sub(r'(RT ){0,1}@[\w\d]*?:{0,1} ', '', line) # user less
#         line = re.sub(r'(RT ){0,1}@[\w\d]*?:{0,1} ', '', line) # hashtag less
#         url_pattern = r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb]))'
#         line = re.sub(url_pattern, '<URL>', line) # swap urls
#         line = re.sub(r'<URL>( <URL>){1,}', '<URL>', line) # swap urls
#         if re.findall(r'#\S*', line): continue # hashtaged line delete
#         line = re.sub(r'&\w{0,};', '', line) # &gt; less
#         line = re.sub(r'quot;', '', line) # quot; less
#         #----------------
#         #----------------
#         line = re.sub(r'…', r'...', line) # … -> ... (2026)
#         line = re.sub(r'\?{1,}![\?!]+', r'?!', line) # ?!???!! -> ?!
#         line = re.sub(r'!{1,}\?[\?!]+', r'?!', line) # ?!???!! -> ?!
#         line = re.sub(r'!{3,}', r'!!', line) # !!!!!!!! -> !!
#         line = re.sub(r'\?{3,}', r'??', line) # ???? -> ??
#         line = re.sub(r'\.{4,}', r'...', line) # ....... -> ...
#         line = re.sub(r'[“”«»]', r'"', line) #
#         line = re.sub(r"’", r"'", line) #
#         line = re.sub(r"[`']{2,}", r'"', line) #
#         line = re.sub(r'[‐‑‒–—―-]{1,}', r'-', line) # (2010)(2011)(2012)(2013)(2014)(2015)(2016) -> - (2012)
#         #----------------
#         #----------------
#         line = re.sub(r"\s+", r" ", line)
#         line = re.sub(r'­', r'', line) # remove u00ad
#         lines.append(line)
#     return lines

# def preproc_step1(data):
#     lines = []
#     for line in data:
#         line = line.strip()
#         #----------------
#         #----------------
#         line = re.sub(r'&\w{0,};', '', line) # &gt; less
#         line = re.sub(r'quot;', '', line) # quot; less
#         #----------------
#         #----------------
#         line = re.sub(r'…', r'...', line) # … -> ... (2026)
#         line = re.sub(r'\?{1,}![\?!]+', r'?!', line) # ?!???!! -> ?!
#         line = re.sub(r'!{1,}\?[\?!]+', r'?!', line) # ?!???!! -> ?!
#         line = re.sub(r'!{3,}', r'!!', line) # !!!!!!!! -> !!
#         line = re.sub(r'\?{3,}', r'??', line) # ???? -> ??
#         line = re.sub(r'\.{4,}', r'...', line) # ....... -> ...
#         line = re.sub(r'[“”«»]', r'"', line) #
#         line = re.sub(r"’", r"'", line) #
#         line = re.sub(r"[`']{2,}", r'"', line) #
#         line = re.sub(r'[‐‑‒–—―-]{1,}', r'-', line) # (2010)(2011)(2012)(2013)(2014)(2015)(2016) -> - (2012)
#         #----------------
#         #----------------
#         line = re.sub(r"\s+", r" ", line)
#         line = re.sub(r'­', r'', line) # remove u00ad
#         lines.append(line)
#     return lines


# url_pattern = r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb]))'
# url_pattern = r'(?:(https?|ftp):\/\/|www\d{0,3}[.]|[\w\d.\-]+[.][\w]{2,6}\/\S*)'
# url_pattern = r'(?:(https?|ftp):\/\/|www\d{0,3}[.]|[\w\d.\-]+[.][A-zА-я]{2,6}\/\S*)'
url_pattern = r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]{};:\'".,<>?\xab\xbb]))'
user_pattern = r'((RT ){0,1}@[\w\d]*?:{0,1}) '
tag_pattern = r'#[\S]*'
def get_rec_info(in_line):
    line = in_line.strip()
    found_urls = re.findall(url_pattern, line)
    found_urls = [match[0] for match in found_urls]
    found_users = re.findall(user_pattern, line)
    found_users = [match[0] for match in found_users]
    found_tags = re.findall(tag_pattern, line)
    recovery_info = {'found_urls':found_urls,
                   'found_users':found_users,
                   'found_tags':found_tags,
                    }
    return recovery_info

def spec_tok_add(in_line):
    line = in_line.strip()
    line = re.sub(url_pattern, ' <URL> ', line) # swap urls
    line = re.sub(user_pattern, ' <USR> ', line) # swap urls
    line = re.sub(tag_pattern, ' <HASHTAG> ', line) # swap urls
    line = line.strip()
    return line

def normalization(in_line):
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

# def get_rec_info(in_lines):
#     url_pattern = r'((?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb])))'
#     user_pattern = r'((RT ){0,1}@[\w\d]*?:{0,1}) '
#     tag_pattern = r'#[\S]*'
#     recovery_info = []
#     for line in in_lines:
#         line = line.strip()
#         found_urls = re.findall(url_pattern, line)
#         found_urls = [match[0] for match in found_urls]
#         found_users = re.findall(user_pattern, line)
#         found_users = [match[0] for match in found_users]
#         found_tags = re.findall(tag_pattern, line)
#         recovery_info.append({'found_urls':found_urls,
#                        'found_users':found_users,
#                        'found_tags':found_tags,
#                         })
#     return recovery_info
#
# def spec_tok_add(in_lines):
#     url_pattern = r'((?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb])))'
#     user_pattern = r'((RT ){0,1}@[\w\d]*?:{0,1}) '
#     tag_pattern = r'#[\S]*'
#     lines = []
#     for line in in_lines:
#         line = line.strip()
#         line = re.sub(url_pattern, ' <URL> ', line) # swap urls
#         line = re.sub(user_pattern, ' <USR> ', line) # swap urls
#         line = re.sub(tag_pattern, ' <HASHTAG> ', line) # swap urls
#         line = line.strip()
#         lines.append(line)
#     return lines
#
# def spec_tok_add(in_lines):
#     url_pattern = r'((?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s()<>]|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:\'".,<>?\xab\xbb])))'
#     user_pattern = r'((RT ){0,1}@[\w\d]*?:{0,1}) '
#     tag_pattern = r'#[\S]*'
#     recovery_info = []
#     lines = []
#     for line in in_lines:
#         line = line.strip()
#         found_urls = re.findall(url_pattern, line)
#         found_urls = [match[0] for match in found_urls]
#         line = re.sub(url_pattern, ' <URL> ', line) # swap urls
#         found_users = re.findall(user_pattern, line)
#         found_users = [match[0] for match in found_users]
#         line = re.sub(user_pattern, ' <USR> ', line) # swap urls
#         found_tags = re.findall(tag_pattern, line)
#         line = re.sub(tag_pattern, ' <HASHTAG> ', line) # swap urls
#         line = line.strip()
#         recovery_info.append({'found_urls':found_urls,
#                        'found_users':found_users,
#                        'found_tags':found_tags,
#                         })
#         lines.append(line)
#     return lines, recovery_info

def gen(elms):
    for e in elms:
        yield e

def recovery(in_row):
    in_line, rec = in_row['text'], in_row['rec']
    line = in_line.strip()
    tokens = line.split()
    urls_gen = gen(rec['found_urls'])
    users_gen = gen(rec['found_users'])
    tags_gen = gen(rec['found_tags'])
    urls_sample = next(urls_gen, None)
    users_sample = next(users_gen, None)
    tags_sample = next(tags_gen, None)
    out_tokens = []
    for tok in tokens:
        if urls_sample and tok=='<URL>':
            out_tokens.append(urls_sample)
            urls_sample = next(urls_gen, None)
        elif users_sample and tok=='<USR>':
            out_tokens.append(users_sample)
            users_sample = next(users_gen, None)
        elif tags_sample and tok=='<HASHTAG>':
            out_tokens.append(tags_sample)
            tags_sample = next(tags_gen, None)
        else:
            out_tokens.append(tok)
    line = ' '.join(out_tokens)
    return line
#
# def recovery(in_lines, recovery_info):
#     lines = []
#     def gen(elms):
#         for e in elms:
#             yield e
#     for rec, line in zip(recovery_info, in_lines):
#         line = line.strip()
#         tokens = line.split()
#         urls_gen = gen(rec['found_urls'])
#         users_gen = gen(rec['found_users'])
#         tags_gen = gen(rec['found_tags'])
#         urls_sample = next(urls_gen, None)
#         users_sample = next(users_gen, None)
#         tags_sample = next(tags_gen, None)
#         out_tokens = []
#         for tok in tokens:
#             if urls_sample and tok=='<URL>':
#                 out_tokens.append(urls_sample)
#                 urls_sample = next(urls_gen, None)
#             elif users_sample and tok=='<USR>':
#                 out_tokens.append(users_sample)
#                 users_sample = next(users_gen, None)
#             elif tags_sample and tok=='<HASHTAG>':
#                 out_tokens.append(tags_sample)
#                 tags_sample = next(tags_gen, None)
#             else:
#                 out_tokens.append(tok)
#         lines.append(' '.join(out_tokens))
#     return lines




def preproc_step2(data):
    model = Model.load(modelfile4udpipe)
    pipeline = Pipeline(model, 'tokenize', Pipeline.DEFAULT, Pipeline.DEFAULT, 'horizontal')
    combining_characters = dict.fromkeys([c for c in range(sys.maxunicode)
                                                   if unicodedata.combining(chr(c))])
    sanitized_lines = []
    for lines in data:
        sentences = pipeline.process(lines).split('\n')
        sanitized_sentences = []
        for sentence in sentences:
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
        sanitized_lines.extend(sanitized_sentences)
    return sanitized_lines


def multi_preprocessing(filepath):
    try:
        zipfile = bz2.BZ2File(filepath) # open the file
        data = zipfile.read().decode() # get the decompressed data
        lines = data.split('\n')
        lines =  preproc_step0(lines)
        # lines =  preproc_step1(lines)
        # lines =  preproc_step2(lines)
        return lines
    except Exception as exc:
        print(exc)
        return []

def timeouted_multi_preprocessing(filepath):
    timeout_duration = 3 * 60 * 60 # 3 hours
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError()
    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)
    try:
        lines = multi_preprocessing(filepath)
    except TimeoutError as exc:
        print(exc)
        lines = []
    finally:
        signal.alarm(0)
    return lines

def run_preproc_pool(json_bz2_files, path2udp_model, cpu_n = 1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, initializer, initargs=[path2udp_model]) as p:
        lines = []
        for process_lines in tqdm.tqdm(p.imap_unordered(multi_preprocessing, json_bz2_files), total=len(json_bz2_files)):
            lines.extend(process_lines)
    return lines

def timeouted_run_preproc_pool(json_bz2_files, path2udp_model, cpu_n = 1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, initializer, initargs=[path2udp_model]) as p:
        lines = []
        for process_lines in tqdm.tqdm(p.imap_unordered(timeouted_multi_preprocessing, json_bz2_files), total=len(json_bz2_files)):
            lines.extend(process_lines)
    return lines
