#!/usr/bin/python3
# -*- coding: utf-8 -*-

# import os
import argparse
# import tqdm
# import shutil

import dask
import dask.bag as db
from dask.diagnostics import ProgressBar
import pandas as pd
from multiprocessing.pool import ThreadPool

from utils import dask_func as dfunc

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--from_dir_prefix')
    parser.add_argument('-t','--to_dir_prefix')
    parser.add_argument('-u','--path2udp_model', default = 'russian-syntagrus-ud-2.0-170801.udpipe')
    parser.add_argument('-n','--cpu_n', default = 5, type = int)
    args = parser.parse_args()
    # dfunc.MODELFILE4UDPIPE = args.path2udp_model
    # dfunc.set_model(args.path2udp_model)
    with dask.config.set(pool=ThreadPool(args.cpu_n)):
        bag = db.read_text(args.from_dir_prefix)
        pbar = ProgressBar()
        pbar.register()
        ddf = bag.to_dataframe(columns=['text'])
        ddf['text'] = ddf['text'].apply(dfunc.skip_empty, meta=('x', 'f8'))
        ddf = ddf.dropna()
        ddf['rec'] = ddf['text'].apply(dfunc.get_rec_info, meta=('x', 'f8'))
        ddf['text'] = ddf['text'].apply(dfunc.spec_tok_add, meta=('x', 'f8'))
        ddf['norm_text'] = ddf['text'].apply(dfunc.normalization1, meta=('x', 'f8'))
        # udpipe_sent_and_tok = dfunc.get_udpipe_sent_and_tok(args.path2udp_model)
        # udpipe_sent_and_tok = dfunc.get_udpipe_sent_and_tok('/home/den/Documents/elmo/data_preparing/rutwitter/russian-syntagrus-ud-2.0-170801.udpipe')
        ddf['norm_text'] = ddf['text'].apply(dfunc.udpipe_sent_and_tok, meta=('x', 'f8'))
        # ddf['norm_text'] = ddf['text'].apply(dfunc.nltk_sent_and_tok, meta=('x', 'f8'))
        ddf['norm_text'] = ddf['norm_text'].apply(dfunc.normalization2, meta=('x', 'f8'))
        ddf['rec_text'] = ddf.apply(dfunc.recovery, meta=('x', 'f8'), axis=1)
        ddf['cleaned_text'] = ddf['norm_text'].apply(dfunc.lower_case, meta=('x', 'f8'))
        ddf[['rec_text', 'cleaned_text']].to_csv(args.to_dir_prefix)

if __name__ == '__main__':
    main()
