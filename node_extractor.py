#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse
import tqdm
import shutil

import utils.tar as tar_utils
import utils.multipreprocessing as mp_utils

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--node_dir', default = 'src/dgx1')
    parser.add_argument('-u','--path2udp_model', default = 'russian-syntagrus-ud-2.0-170801.udpipe')
    parser.add_argument('-n','--cpu_n', default = 5, type = int)
    args = parser.parse_args()

    # init pathes
    links_dir = os.path.join(args.node_dir, 'links')
    tmp_dir = os.path.join(args.node_dir, 'tmp')
    results_dir = os.path.join(args.node_dir, 'results')
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)
    # get tar files
    tar_files = tar_utils.get_tar_files(links_dir)

    # main for
    for tar_file in tqdm.tqdm(tar_files):
        try:
            print('Extracting from {}'.format(tar_file))
            dir4extracts = tar_utils.extract_tar(tmp_dir, tar_file)
            json_bz2_files = mp_utils.get_json_bz2_files(dir4extracts)

            print('Running pool: {} json_bz2_files and {} cpu units'.format(len(json_bz2_files),
                                                                    args.cpu_n))
            txt_lines = mp_utils.timeouted_run_preproc_pool(json_bz2_files, args.path2udp_model,
                                                                    args.cpu_n)

            print('Saving all text lines...')
            txt_output_file = os.path.join(results_dir,
                                            os.path.split(dir4extracts)[-1] + '.txt')
            with open(txt_output_file, 'wt', encoding="utf-8") as output_f:
                for line in txt_lines[:-1]:
                    output_f.write(line + '\n')
                output_f.write(txt_lines[-1])
            shutil.rmtree(dir4extracts)
            os.remove(tar_file)
        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    main()
