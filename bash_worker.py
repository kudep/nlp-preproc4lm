#!/usr/bin/python3
# -*- coding: utf-8 -*-

import pathlib
import os
import argparse
from glob import glob

# from ufal.udpipe import Model, Pipeline
# from utils import multi


import os
import re
import tqdm
from multiprocessing import Pool

import utils.functions as func

import traceback

import collections

import json
import bz2
import pandas as pd
from datetime import datetime


def extract_json_data(data):
    if data.get('lang') == 'ru' and 'text' in data and 'created_at' in data:
        extracted_data = {}
        if data.get('quoted_status_id') and 'text' in data.get('quoted_status', {}):
            extracted_data['quoted_text'] = data['quoted_status']['text']
            extracted_data['quoted_status_id'] = data['quoted_status_id']
        # else:
        #     extracted_data['in_reply_to_status_id'] = 0

        if data.get('in_reply_to_status_id'):
            extracted_data['in_reply_to_status_id'] = data['in_reply_to_status_id']
        # else:
        #     extracted_data['in_reply_to_status_id'] = 0

        extracted_data['id'] = data['id']
        extracted_data['text'] = data['text']
        extracted_data['created_at'] =\
            datetime.strptime(data['created_at'], '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S')
        return extracted_data


def FLAGS(): return None


def timeout_initializer(timeout_duration):
    global FLAGS
    FLAGS.timeout_duration = timeout_duration


def worker(inout_file):
    in_file, out_file = inout_file
    try:
        if in_file[-3:] != 'bz2':
            return
        try:
            lines = bz2.open(in_file).readlines()
        except:
            return

        fin_data = []
        for line in lines:
            data = json.loads(line)
            ext_data = extract_json_data(data)
            if ext_data:
                fin_data.append(ext_data)
        return fin_data
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
        return worker(inout_file)
    except TimeoutError as exc:
        print(exc)
    finally:
        signal.alarm(0)


def run_pool(files, cpu_n=1):
    # Using initializer and  multi_preprocessing functions from this module
    fin_data = []
    with Pool(cpu_n) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, files), total=len(files)):
            if process_ret:
                fin_data.extend(process_ret)
    return fin_data


def timeouted_run_pool(files, cpu_n=1, timeout_duration=40*60):
    # Using initializer and  multi_preprocessing functions from this module
    fin_data = []
    with Pool(cpu_n, timeout_initializer, initargs=[timeout_duration]) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(timeouted_worker, files), total=len(files)):
            if process_ret:
                fin_data.extend(process_ret)
    return fin_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--from_dir_pattern', type=str)
    parser.add_argument('-t', '--to_file', default='/tmp/workers', type=str)
    parser.add_argument('-n', '--cpu_n', default=5, type=int)
    parser.add_argument('-T', '--timeout_duration', default=2*60*60, type=int)  # timeout_duration = 2 hours
    args = parser.parse_args()

    from_files = glob(args.from_dir_pattern)
    from_files = [str(pathlib.Path(file).resolve()) for file in from_files]
    files = list(zip(from_files, from_files))

    fin_data = timeouted_run_pool(files, cpu_n=args.cpu_n,
                                  timeout_duration=args.timeout_duration)

    df = pd.DataFrame(fin_data, columns=['created_at',
                                         'id',
                                         'text',
                                         'quoted_text',
                                         'quoted_status_id',
                                         'in_reply_to_status_id',
                                         ])
    df_ids = df[['id', 'quoted_status_id', 'in_reply_to_status_id']]
    # df.to_csv(args.to_file, index=False, header=False)
    df.to_pickle(args.to_file)
    # df_ids.to_csv(args.to_file[:-4]+'.ids.csv', index=False, header=False)


if __name__ == '__main__':
    main()
