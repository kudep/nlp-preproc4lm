#!/usr/bin/python3
# -*- coding: utf-8 -*-


import tqdm
from multiprocessing import Pool


def run_map(function, in_list):
    return list(map(function, in_list))


def run_pool(worker_func, files, cpu_n=1, ret_handler=lambda x: None):
    # Using initializer and  multi_preprocessing functions from this module
    with Pool(cpu_n) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(worker_func, files), total=len(files)):
            ret_handler(process_ret)
