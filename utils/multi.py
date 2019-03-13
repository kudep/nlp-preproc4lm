#!/usr/bin/python3
# -*- coding: utf-8 -*-


import json
import traceback
from multiprocessing import Pool
import tqdm

import utils.functions as fn


def worker(args):
    try:
        in_file, out_file, alphabet, vocab = args
        docs = json.load(in_file.open())
        sentences = fn.docs2sentences(docs, alphabet, vocab)
        with out_file.open("wt") as fd:
            fn.run_map(lambda line: fd.write("%s\n" % str(line).strip()), sentences)

    except Exception:
        print("Exception in file {}".format(in_file))
        traceback.print_exc()


def timeouted_worker(args):
    import signal

    in_file, out_file, alphabet, vocab, timeout_duration = args

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError("Timeout error in file %s" % in_file)

    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(timeout_duration)
    try:
        word_count = worker((in_file, out_file, alphabet, vocab))
    except TimeoutError as exc:
        print(exc)
        word_count = None
    finally:
        signal.alarm(0)
    return word_count


def run_pool(worker, worker_args, cpu_n=1):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n) as p:
        rets = []
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, worker_args), total=len(worker_args)):
            rets.append(process_ret)
    return rets
