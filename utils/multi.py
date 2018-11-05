#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
import tqdm
from multiprocessing import Pool

import utils.functions as func

import traceback

import collections

FLAGS = lambda : None

def initializer(udpipeline, out_dir_prefix):
    global FLAGS
    FLAGS.udpipeline = udpipeline
    FLAGS.out_dir_prefix = out_dir_prefix

def timeout_initializer(udpipeline, out_dir_prefix, timeout_duration):
    initializer(udpipeline, out_dir_prefix)
    global FLAGS
    FLAGS.timeout_duration = timeout_duration

def run_map(function, in_list):
    return list(map(function, in_list))

def worker(in_file):
    try:
        out_file = FLAGS.out_dir_prefix + '/' + in_file.split('/')[-1]
        df = {'text':open(in_file).readlines()}
        df['text'] = run_map(func.skip_spaced_line, df['text'])
        df['text'] = [line for line in df['text'] if line]
        df['text'] = run_map(func.skip_not_char_line, df['text'])
        df['text'] = [line for line in df['text'] if line]

        #Text normalization
        df['norm_text'] = run_map(func.normalization1, df['text'])
        df.pop('text', None)
        udpipe_sent_and_tok = func.get_udpipe_sent_and_tok(FLAGS.udpipeline)
        df['norm_text'] = run_map(udpipe_sent_and_tok, df['norm_text'])
        df['norm_text'] = run_map(func.normalization2, df['norm_text'])

        #Prepare to save
        df['norm_text'] = func.split_lines(df['norm_text'], '\n')
        df['norm_text'] = [line for line in df['norm_text'] if line]
        df['norm_text'] = list(set(df['norm_text']))
        with open(out_file+'.rec', 'wt') as fd:
            run_map(lambda line: fd.write('%s\n' % str(line).strip()), df['norm_text'][:-1])
            run_map(lambda line: fd.write('%s\n' % str(line).strip()), df['norm_text'][-1:])

        #Count to words
        word_counts = run_map(lambda line: collections.Counter(line.strip().split()), df['norm_text'])
        word_counts = func.counters_merge(word_counts)
        word_counts = word_counts if word_counts else collections.Counter()
        return word_counts
    except Exception:
        print('Exception in file {}'.format(in_file))
        traceback.print_exc()
        return collections.Counter()

def timeouted_worker(in_file):
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError('Timeout error in file %s' % in_file)
    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(FLAGS.timeout_duration)
    try:
        word_count = worker(in_file)
    except TimeoutError as exc:
        print(exc)
        word_count = collections.Counter()
    finally:
        signal.alarm(0)
    return word_count

def run_pool(in_files, udpipeline, out_dir_prefix, cpu_n = 1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, initializer, initargs=[udpipeline, out_dir_prefix]) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, in_files), total=len(in_files)):
            rets.append(process_ret)
    return rets

def timeouted_run_pool(in_files, udpipeline, out_dir_prefix, cpu_n = 1, timeout_duration=40*60):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, timeout_initializer, initargs=[udpipeline, out_dir_prefix, timeout_duration]) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(timeouted_worker, in_files), total=len(in_files)):
            rets.append(process_ret)
    return rets
