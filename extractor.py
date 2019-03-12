#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse
from glob import glob
import pathlib


from utils import multi
from utils import functions as fn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--from_dir_pattern", type=str)
    parser.add_argument("-t", "--to_dir_prefix", default="/tmp/workers", type=str)
    parser.add_argument("-n", "--cpu_n", default=5, type=int)
    parser.add_argument("-T", "--timeout_duration", default=2 * 60 * 60, type=int)  # timeout_duration = 2 hours
    args = parser.parse_args()

    os.makedirs(args.to_dir_prefix, exist_ok=True)

    files = glob(args.from_dir_pattern)
    files = fn.run_map(pathlib.Path, files)

    multi.timeouted_run_pool(
        files, pathlib.Path(args.to_dir_prefix), cpu_n=args.cpu_n, timeout_duration=args.timeout_duration
    )


if __name__ == "__main__":
    main()
