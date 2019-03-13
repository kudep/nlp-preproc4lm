#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import random
from glob import glob
import pathlib

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
    parser.add_argument("-l", "--line_per_file", default=100000, type=int)
    args = parser.parse_args()

    to_dir_prefix = pathlib.Path(args.to_dir_prefix)
    to_dir_prefix.mkdir(parents=True, exist_ok=True)

    in_files = glob(args.from_dir_pattern)
    in_files = fn.run_map(pathlib.Path, in_files)
    data_lines = []
    fn.run_map(lambda f: data_lines.extend(f.open().readlines()), in_files)
    random.shuffle(data_lines)
    data_chunks = fn.chunk_generator(data_lines, args.line_per_file)
    for i, data_chunk in enumerate(data_chunks):
        to_file = to_dir_prefix / f'part_{i:04d}'
        data_chunk = [line.strip() for line in data_chunk]
        to_file.open('wt').write('\n'.join(data_chunk))


if __name__ == "__main__":
    main()
