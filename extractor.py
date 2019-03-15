#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pathlib
import random
import json

from_dir = pathlib.Path("/home/den/Documents/chit-chat_2019/data/alice_dialogs")
to_dir = from_dir / "tgt"
in_csv1 = from_dir / "Alice2Alice (2).csv"
in_csv2 = from_dir / "Alice2AliceFromAggregatedDialogs1.csv"
in_csv3 = from_dir / "Alice2AliceFromAggregatedDialogs2.csv"
in_json = from_dir / "all.json"


def drop_repeated_utters(dialog):
    dialog = dialog + ["#@*1", "#@*2", "#@*3"]
    cleaned_dialog = []
    for ut1, hyp1, ut2, hyp2 in zip(dialog[::2], dialog[2::2], dialog[1::2], dialog[3::2]):
        if ut1 == hyp1 and ut2 == hyp2:
            continue
        else:
            cleaned_dialog.append(ut1)
            cleaned_dialog.append(ut2)
    cleaned_dialog = cleaned_dialog if cleaned_dialog[-1] != "#@*1" else cleaned_dialog[:-1]
    return cleaned_dialog


def preproc_dial(dialog):
    cleaned_dialog = []
    for utter in dialog:
        utter = utter.replace("\n", " ").strip().split()
        cleaned_dialog.append(" ".join(utter[-125:]))
    return cleaned_dialog


def dialog_split(data):
    dialogs = [[]]
    for utter in data[1:]:
        if utter == "\t\n":
            dialogs.append([])
        else:
            dialogs[-1].append(utter[4:])
    dialog_lines = []
    for dialog in dialogs:
        if len(dialog) > 1:
            dialog = drop_repeated_utters(dialog)
            dialog = preproc_dial(dialog)
            dialog_lines.append(list(zip(dialog[::2], dialog[1::2])))
            dialog_lines.append(list(zip(dialog[1::2], dialog[2::2])))
    return dialog_lines


def csv2txt_dialogs(in_csv, out_txt):
    out_txt.parent.mkdir(511, True, True)
    data = in_csv.open("rt", encoding="utf-16le").readlines()
    dialog_lines = dialog_split(data)
    random.shuffle(dialog_lines)
    with out_txt.with_suffix(".train.txt").open("wt") as out_d_train, out_txt.with_suffix(".valid.txt").open(
        "wt"
    ) as out_d_valid:
        for dialog_line in dialog_lines:
            dial_lines = [
                f"{i} {utter1.strip()} \t{utter2.strip()}" for i, (utter1, utter2) in enumerate(dialog_line, 1)
            ]
            if not (dial_lines):
                continue
            if random.random() <= 0.02:
                out_d_valid.write("\n".join(dial_lines))
                out_d_valid.write("\n")
            else:
                out_d_train.write("\n".join(dial_lines))
                out_d_train.write("\n")


def json2txt_dialogs(in_json, out_txt):
    out_txt.parent.mkdir(511, True, True)
    js = json.load(in_json.open())
    dialogs = [dialog["messages"] for dialog in js["data"]]
    dialogs = [list(reversed([msg["text"] for msg in dialog])) for dialog in dialogs]
    dialog_lines = []
    for dialog in dialogs:
        if len(dialog) > 1:
            dialog = preproc_dial(dialog)
            dialog_lines.append(list(zip(dialog[::2], dialog[1::2])))
            dialog_lines.append(list(zip(dialog[1::2], dialog[2::2])))
    random.shuffle(dialog_lines)
    with out_txt.with_suffix(".train.txt").open("wt") as out_d_train, out_txt.with_suffix(".valid.txt").open(
        "wt"
    ) as out_d_valid:
        for dialog_line in dialog_lines:
            dial_lines = [
                f"{i} {utter1.strip()} \t{utter2.strip()}" for i, (utter1, utter2) in enumerate(dialog_line, 1)
            ]
            if not (dial_lines):
                continue
            if random.random() <= 0.02:
                out_d_valid.write("\n".join(dial_lines))
                out_d_valid.write("\n")
            else:
                out_d_train.write("\n".join(dial_lines))
                out_d_train.write("\n")


# %%
csv2txt_dialogs(in_csv1, to_dir / "file1")
csv2txt_dialogs(in_csv2, to_dir / "file2")
csv2txt_dialogs(in_csv3, to_dir / "file3")

json2txt_dialogs(in_json, to_dir / "json_file")
