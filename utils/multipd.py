#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
import tqdm
from multiprocessing import Pool
#next level
# from ufal.udpipe import Model, Pipeline
import utils.pdfunc as func
import pandas as pd

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

def worker(in_file):
    try:
        out_file = FLAGS.out_dir_prefix + '/' + in_file.split('/')[-1]
        df = pd.read_fwf(in_file,  header=None).rename(columns={0:'text'})
        df['text'] = df['text'].apply(func.skip_empty)
        df = df.dropna()
        df['rec'] = df['text'].apply(func.get_rec_info)
        df['text'] = df['text'].apply(func.spec_tok_add)
        df['norm_text'] = df['text'].apply(func.normalization1)
        df['norm_text'] = df['text'].apply(func.udpipe_sent_and_tok, args = (FLAGS.udpipeline,))
        df['norm_text'] = df['norm_text'].apply(func.normalization2)
        df['rec_text'] = df.apply(func.recovery, axis=1)
        df['cleaned_text'] = df['norm_text'].apply(func.lower_case)
        df1 = func.split_df(df[['cleaned_text']],'cleaned_text', '\n')
        df2 = func.split_df(df[['rec_text']],'rec_text', '\n')
        df = pd.concat([df1,df2], axis=1)
        with open(out_file+'.clean', 'wt') as fd:
            df['cleaned_text'][:-1].apply(lambda line: fd.write('%s\n' % line.strip()))
            df['cleaned_text'][-1:].apply(lambda line: fd.write(line.strip()))
        with open(out_file+'.rec', 'wt') as fd:
            df['rec_text'][:-1].apply(lambda line: fd.write('%s\n' % line.strip()))
            df['rec_text'][-1:].apply(lambda line: fd.write(line.strip()))
        word_counts = df['cleaned_text'].apply(lambda line: collections.Counter(line.strip().split()))
        word_counts = word_counts.tolist()
        word_counts = sum(word_counts, collections.Counter())
        return word_counts
    except Exception:
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
