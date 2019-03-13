#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import random
from glob import glob
import pathlib
from multiprocessing import Pool

import tqdm

from utils import functions as fn


def worker(inout_file):
    in_file, out_file, valid_partition = inout_file
    with in_file.open() as in_f, out_file.with_suffix(".valid").open("wt") as valid_out_f, out_file.with_suffix(
        ".train"
    ).open("wt") as train_out_f:
        lines = in_f.readlines()
        random.shuffle(lines)
        for line in lines:
            line = line.strip()
            if not (line):
                continue
            if random.random() < valid_partition:
                valid_out_f.write("%s\n" % str(line))
            else:
                train_out_f.write("%s\n" % str(line))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--from_dir_pattern", type=str)
    parser.add_argument("-t", "--to_dir_prefix", default="/tmp/workers", type=str)
    parser.add_argument("-n", "--cpu_n", default=20, type=int)
    parser.add_argument("-v", "--valid_partition", default=0.01, type=float)
    args = parser.parse_args()

    to_dir_prefix = pathlib.Path(args.to_dir_prefix)
    to_dir_prefix.mkdir(parents=True, exist_ok=True)

    in_files = glob(args.from_dir_pattern)
    in_files = fn.run_map(pathlib.Path, in_files)
    out_files = fn.run_map(lambda x: to_dir_prefix / x.with_suffix(".txt").name, in_files)
    worker_args = list(zip(in_files, out_files, [args.valid_partition for _ in out_files]))

    with Pool(args.cpu_n) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, worker_args), total=len(worker_args)):
            rets.append(process_ret)


if __name__ == "__main__":
    main()
