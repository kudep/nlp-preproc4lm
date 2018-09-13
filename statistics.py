#!/usr/bin/python3
# -*- coding: utf-8 -*-


import argparse
import os
from multiprocessing import Pool
import re
import tqdm
#next level
import unicodedata
import collections
import random


def get_files(dir_name, pattern):
    pattern = re.compile(pattern)
    txt_files = []
    for step in os.walk(dir_name):
         txt_files.extend([os.path.join(step[0],file_name) for file_name in step[-1] if pattern.findall(file_name)])
    return txt_files

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

def count_chars(lines):
    '''
        With a spaces skiping.
    '''
    counters = []
    for line in lines:
        if line:
            counters.append(collections.Counter(''.join(line.split())))
    return [counters_merge(counters)]

def count_tokens(text_lines):
    counters = []
    for line in text_lines:
        if line:
            counters.append(collections.Counter(line.split()))
    return [counters_merge(counters)]

def file_count_tokens(filename):
    text_lines = open(filename).readlines()
    return count_tokens(text_lines)

def file_count_chars(filename):
    text_lines = open(filename).readlines()
    return count_chars(text_lines)

def start_pool(items, func, workers_n):
    with Pool(workers_n) as p:
        results = []
        for worker_results in p.imap_unordered(func, items):
            results.extend(worker_results)
    return results


def create_chars_vocab(txt_files:list, workers_n=1):
    """
    txt_files = list of pathways to files
    """
    chars_counters = start_pool(txt_files, file_count_chars, workers_n)
    chars_counter = counters_merge(chars_counters)
    chars_vocab = [char for char, c in chars_counter.most_common(255)] + ['😟'] #U+1F61F
    return chars_vocab, chars_counter

def create_tokens_vocab(txt_files:list, workers_n=1):
    """
    txt_files = list of pathways to files
    """
    tokens_counters = start_pool(txt_files, file_count_tokens, workers_n)
    tokens_counter = counters_merge(tokens_counters)
    tokens_vocab = ['<S>', '</S>', '<UNK>'] + [token for token, c in tokens_counter.most_common()]
    return tokens_vocab, tokens_counter


def chars_initializer(chars, _path2tgt, _src_len):
    global freq_chars
    freq_chars = chars
    global path2tgt
    path2tgt = _path2tgt
    global src_len
    src_len = _src_len

def file_map_chars(filename):
    text_lines = open(filename).readlines()
    mapper = lambda char: char if char in freq_chars else freq_chars[-1]
    out_lines = []
    out_lines.extend([ ''.join(map(mapper,line)) for line in text_lines if line])

    spec_path = os.path.split(filename)[src_len-1:]
    base_path = os.path.split(path2tgt)
    full_path = base_path + spec_path
    tgt_path = os.path.join(*full_path)
    open(tgt_path, 'wt', encoding="utf-8").write('\n'.join(out_lines))

def map_files(txt_files:list, chars: list, path2tgt: str, src_len: int, workers_n=1):
    """
    txt_files = list of pathways to files
    """

    with Pool(workers_n, initializer = chars_initializer,
                                    initargs = [chars + [' '], path2tgt, src_len]) as p:
        p.map(file_map_chars, txt_files)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s','--src', default = 'data')
    parser.add_argument('-t','--tgt', default = 'res')
    parser.add_argument('-n','--workers_n', default = 5, type = int)

    args = parser.parse_args()
    os.makedirs(args.tgt, exist_ok = True)
    tgt_pathways = {
    'heap':os.path.join(args.tgt, 'heap.txt'),
    'chars':os.path.join(args.tgt, 'chars.txt'),
    'tokens':os.path.join(args.tgt, 'tokens.txt'),
    'chars_counter':os.path.join(args.tgt, 'chars_counter.txt'),
    'tokens_counter':os.path.join(args.tgt, 'tokens_counter.txt')
    }
    files = get_files(args.src, r'prts.*')

    print('Executing chars set...')
    chars_vocab, chars_counter = create_chars_vocab(files, workers_n=args.workers_n)
    # map_files

    print('Saving chars...')
    open(tgt_pathways['chars'], 'wt', encoding="utf-8").write('\n'.join(chars_vocab))
    chars_counter = chars_counter.most_common()
    chars_counter = ['{} {}'.format(char, freq) for char, freq in chars_counter]
    open(tgt_pathways['chars_counter'], 'wt', encoding="utf-8").write('\n'.join(chars_counter))

    print('Mapping chars...')
    map_files(files, chars_vocab, args.tgt, len(os.path.split(args.src)), workers_n=args.workers_n)

    print('Executing tokens set...')
    files = get_files(args.tgt, r'prts.*')
    tokens_vocab, tokens_counter = create_tokens_vocab(files, workers_n=args.workers_n)

    print('Saving tokens...')
    open(tgt_pathways['tokens'], 'wt', encoding="utf-8").write('\n'.join(tokens_vocab))
    tokens_counter = tokens_counter.most_common()
    tokens_counter = ['{} {}'.format(char, freq) for char, freq in tokens_counter]
    open(tgt_pathways['tokens_counter'], 'wt', encoding="utf-8").write('\n'.join(tokens_counter))


if __name__ == '__main__':
    main()
