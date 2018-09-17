#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse

from glob import glob

from ufal.udpipe import Model, Pipeline
from utils import multi

import collections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--from_dir_pattern', type = str)
    parser.add_argument('-t','--to_dir_prefix', default = '/tmp/workers', type = str)
    parser.add_argument('-v','--to_vocab_file', default = '/tmp/workers/vocab.txt', type = str)
    parser.add_argument('-u','--path2udp_model', default = './russian-syntagrus-ud-2.0-170801.udpipe', type = str)
    parser.add_argument('-n','--cpu_n', default = 5, type = int)
    parser.add_argument('-T','--timeout_duration', default = 2*60*60, type = int) # timeout_duration = 2 hours
    args = parser.parse_args()

    os.makedirs(args.to_dir_prefix, exist_ok=True)

    model = Model.load(args.path2udp_model)
    udpipeline = Pipeline(model, 'tokenize', Pipeline.DEFAULT, Pipeline.DEFAULT, 'horizontal')

    files = list(glob(args.from_dir_pattern))

    word_counts = multi.timeouted_run_pool(files,udpipeline,args.to_dir_prefix, cpu_n=args.cpu_n, timeout_duration=args.timeout_duration)
    word_count = sum(word_counts, collections.Counter())

    vocab = [ '%s\n' % word for word, _ in word_count.most_common()]
    vocab.append(word_count.most_common()[-1][0])
    open(args.to_vocab_file, 'wt').writelines(vocab)

    freqs = [ '{}\t{}\n'.format(word, freq) for word, freq in word_count.most_common()[:-1]]
    open(args.to_vocab_file+'.freqs', 'wt').writelines(freqs)

if __name__ == '__main__':
    main()
