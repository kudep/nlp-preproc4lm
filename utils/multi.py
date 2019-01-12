#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import re
import tqdm
from multiprocessing import Pool

import utils.functions as func

import traceback

import collections

import json


def FLAGS(): return None


def timeout_initializer(timeout_duration):
    global FLAGS
    FLAGS.timeout_duration = timeout_duration


def run_map(function, in_list):
    return list(map(function, in_list))


def worker(inout_file):
    in_file, out_file = inout_file
    try:
        df = {'docs': in_file.open().readlines()}
        df['docs'] = run_map(json.loads, df['docs'])
        df['docs'] = run_map(func.doc2sentences, df['docs'])

        out_file.parent.mkdir(parents=True, exist_ok=True)
        with out_file.open('wt') as fd:
            for doc in df['docs'][:-1]:
                run_map(lambda line: fd.write('%s\n' % str(line).strip()), doc)
                fd.write('\n')
            run_map(lambda line: fd.write('%s\n' % str(line).strip()), df['docs'][-1])
    except Exception:
        print('Exception in file {}'.format(in_file))
        traceback.print_exc()


def timeouted_worker(inout_file):
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError('Timeout error in file %s' % inout_file[0])
    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(FLAGS.timeout_duration)
    try:
        worker(inout_file)
    except TimeoutError as exc:
        print(exc)
    finally:
        signal.alarm(0)


def run_pool(files, cpu_n=1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, files), total=len(files)):
            pass


def timeouted_run_pool(files, cpu_n=1, timeout_duration=40*60):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, timeout_initializer, initargs=[timeout_duration]) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(timeouted_worker, files), total=len(files)):
            pass
