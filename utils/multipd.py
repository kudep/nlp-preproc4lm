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
        # df = pd.read_fwf(in_file,  header=None).rename(columns={0:'text'})
        df = {'text':open(in_file).readlines()}
        df['text'] = map(func.skip_empty, df['text'])
        df['text'] = [line for line in df if line]
        df['rec'] =  map(func.get_rec_info, df['text'])
        df['text'] = map(func.spec_tok_add, df['text'])

        #Text normalization
        df['norm_text'] = map(func.normalization1, df['text'])
        df.pop('text', None)
        udpipe_sent_and_tok = func.get_udpipe_sent_and_tok(udpipeline)
        df['norm_text'] = map(func.udpipe_sent_and_tok, df['norm_text'])
        df['norm_text'] = map(func.normalization2, df['norm_text'])
        df['rec_text'] = map(func.recovery, zip(df['norm_text'],df['rec']))
        df['cleaned_text'] = map(func.lower_case, df['norm_text'])
        df.pop('norm_text', None)

        #Prepare to save
        df['cleaned_text' = func.split_lines(df['cleaned_text'], '\n')
        df['rec_text' = func.split_lines(df['rec_text'], '\n')
        with open(out_file+'.clean', 'wt') as fd:
            df['cleaned_text'][:-1].apply(lambda line: fd.write('%s\n' % line.strip()))
            df['cleaned_text'][-1:].apply(lambda line: fd.write(line.strip()))
        with open(out_file+'.rec', 'wt') as fd:
            df['rec_text'][:-1].apply(lambda line: fd.write('%s\n' % line.strip()))
            df['rec_text'][-1:].apply(lambda line: fd.write(line.strip()))
        df.pop('rec_text', None)

        #Count to words
        word_counts = map(lambda line: collections.Counter(line.strip().split()), df['cleaned_text'])
        word_counts = list(word_counts)
        word_counts = sum(word_counts, collections.Counter())
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
