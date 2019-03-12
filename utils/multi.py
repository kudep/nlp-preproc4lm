#!/usr/bin/python3
# -*- coding: utf-8 -*-


import json
import traceback
from multiprocessing import Pool

import tqdm

import utils.functions as func


def FLAGS():
    return None


def initializer(out_dir_prefix):
    global FLAGS
    FLAGS.out_dir_prefix = out_dir_prefix


def timeout_initializer(out_dir_prefix, timeout_duration):
    initializer(out_dir_prefix)
    global FLAGS
    FLAGS.timeout_duration = timeout_duration


def worker(in_file):
    try:
        out_file = FLAGS.out_dir_prefix + "/" + in_file.split("/")[-1]
        docs = json.load(open(in_file))
        sentences = func.docs2sentences(docs)
        with open(out_file + ".rec", "wt") as fd:
            map(lambda line: fd.write("%s\n" % str(line).strip()), sentences)

    except Exception:
        print("Exception in file {}".format(in_file))
        traceback.print_exc()


def timeouted_worker(in_file):
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError("Timeout error in file %s" % in_file)

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(FLAGS.timeout_duration)
    try:
        word_count = worker(in_file)
    except TimeoutError as exc:
        print(exc)
        word_count = None
    finally:
        signal.alarm(0)
    return word_count


def run_pool(in_files, out_dir_prefix, cpu_n=1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, initializer, initargs=[out_dir_prefix]) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, in_files), total=len(in_files)):
            rets.append(process_ret)
    return rets


def timeouted_run_pool(in_files, out_dir_prefix, cpu_n=1, timeout_duration=40 * 60):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n, timeout_initializer, initargs=[out_dir_prefix, timeout_duration]) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(timeouted_worker, in_files), total=len(in_files)):
            rets.append(process_ret)
    return rets
