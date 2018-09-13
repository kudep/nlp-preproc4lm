#!/usr/bin/python3
# -*- coding: utf-8 -*-


import os
import tarfile



def get_tar_files(dir_name):
    tar_files = []
    for step in os.walk(dir_name):
         tar_files.extend([os.path.join(step[0],i) for i in step[-1] if os.path.splitext(i)[-1] == '.tar'])
    return tar_files

def extract_tar(tmp_dir, tar_file):
    tarfile_basename = os.path.splitext(os.path.basename(tar_file))[0]
    dir4extracts = os.path.join(tmp_dir, tarfile_basename)
    os.makedirs(dir4extracts, exist_ok=True)
    tar = tarfile.open(tar_file)
    tar.extractall(path=dir4extracts)
    tar.close()
    return dir4extracts
