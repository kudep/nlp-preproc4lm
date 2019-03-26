#!/usr/bin/python3
# -*- coding: utf-8 -*-
# %%

import pathlib
import argparse
import glob
import traceback

import supply_utils.multi as multi
import supply_utils.functions as funcs

import collections

# %load_ext autoreload

# %autoreload 2


def worker(args):
    in_file = args
    try:
        df = {"utters": funcs.bz2file2utters(in_file)}
        df["utters"] = funcs.utters2utters(df["utters"])
        return df["utters"]
    except Exception:
        print("Exception in file {}".format(in_file))
        traceback.print_exc()
        return []


def timeout_worker(args):
    args, timeout_duration = args
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError(f"Timeout error with args {args} ")

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)
    try:
        return worker(args)
    except TimeoutError as exc:
        print(exc)
        return []
    finally:
        signal.alarm(0)


def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("-d", "--from_dir_pattern", type=str)
    parser.add_argument(
        "-d", "--from_dir_pattern", default="/tmp/workers/*.json.bz2", type=str
    )
    parser.add_argument("-t", "--to_file", default="/tmp/workers/txt", type=str)
    parser.add_argument("-n", "--cpu_n", default=5, type=int)
    parser.add_argument("-T", "--timeout_duration", default=2 * 60, type=int)  # timeout_duration = 2 min
    args = parser.parse_args()

    to_file = pathlib.Path(args.to_file)
    to_file.parent.mkdir(parents=True, exist_ok=True)

    from_files = glob.glob(args.from_dir_pattern)
    from_files = [pathlib.Path(file).resolve() for file in from_files]
    in_args = [(file, args.timeout_duration) for file in from_files]

    with to_file.open("wt") as out_file:

        def write2file(lines):
            for line in lines:
                out_file.write(f"{line}\n")

        multi.run_pool(timeout_worker, in_args, cpu_n=args.cpu_n, ret_handler=write2file)


if __name__ == "__main__":
    main()
