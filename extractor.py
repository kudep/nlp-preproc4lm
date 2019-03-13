#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
from glob import glob
import pathlib
import collections


from utils import multi
from utils import functions as fn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--from_dir_pattern", type=str)
    parser.add_argument("-t", "--to_dir_prefix", default="/tmp/workers", type=str)
    parser.add_argument("-v", "--vocab_file", default="./tokens_set.txt", type=str)
    parser.add_argument("-n", "--cpu_n", default=5, type=int)
    parser.add_argument("-T", "--timeout_duration", default=2 * 60 * 60, type=int)  # timeout_duration = 2 hours
    args = parser.parse_args()

    to_dir_prefix = pathlib.Path(args.to_dir_prefix)
    tokens = pathlib.Path(args.vocab_file).open().readlines()
    chars_counters = [collections.Counter(token.strip()) for token in tokens]
    vocab = set(tokens)
    chars_counter = fn.counters_merge(chars_counters)
    chars_counter.update(" ")

    to_dir_prefix.mkdir(parents=True, exist_ok=True)

    in_files = glob(args.from_dir_pattern)
    in_files = fn.run_map(pathlib.Path, in_files)
    out_files = fn.run_map(lambda x: to_dir_prefix / x.with_suffix(".txt").name, in_files)
    worker_args = list(
        zip(
            in_files,
            out_files,
            [chars_counter for _ in out_files],
            [vocab for _ in out_files],
            [args.timeout_duration for _ in out_files],
        )
    )

    multi.run_pool(multi.timeouted_worker, worker_args, cpu_n=args.cpu_n)


if __name__ == "__main__":
    main()
