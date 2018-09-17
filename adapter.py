#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse

from glob import glob
import json

from utils import multi
from utils import functions as func

import tqdm

from multiprocessing import Pool

FLAGS = lambda : None
FLAGS.to_dir_prefix = None
def initializer(_to_dir_prefix):
    global FLAGS
    FLAGS.to_dir_prefix = _to_dir_prefix

def chunked_files_func(_args):
    index, files = _args[0], _args[1]
    with open(str(FLAGS.to_dir_prefix) +'/part_'+str(index).zfill(7), 'wt', encoding="utf-8") as out_file:

        def line_func(line):
            line = str(json.loads(line).get('text','')).strip()
            if line:
                out_file.write(line)

        def file_func(file):
            lines = open(file, 'r', encoding="utf-8").readlines()
            multi.run_map(line_func,lines)

        multi.run_map(file_func, files)
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--from_dir_pattern', type = str)
    parser.add_argument('-t','--to_dir_prefix', default = '/tmp/workers', type = str)
    parser.add_argument('-n','--cpu_n', default = 5, type = int)
    args = parser.parse_args()

    os.makedirs(args.to_dir_prefix, exist_ok=True)

    files = list(glob(args.from_dir_pattern))
    chunk_size = 10
    gen = func.chunk_generator(files, chunk_size)
    chunked_files = list(enumerate(gen))

    # initializer(args.to_dir_prefix)
    # for _ in tqdm.tqdm(map(chunked_files_func, chunked_files), total=len(chunked_files)):
    #     pass

    with Pool(args.cpu_n, initializer, initargs=[args.to_dir_prefix]) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(chunked_files_func, chunked_files), total=len(chunked_files)):
            # rets.append(process_ret)
            pass


if __name__ == '__main__':
    main()
