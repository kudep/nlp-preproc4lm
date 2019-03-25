#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pathlib
import random
import json
import collections
import pprint

from_dir = pathlib.Path("/home/den/Documents/chit-chat_2019/data/alice_dialogs")
to_dir = from_dir / "tgt"
to_dir = from_dir / "tgt_2019_03_25"
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
            dialogs[-1].append(utter.strip().split("\t")[-1])
    new_dialogs = []
    for utters in dialogs:
        while utters and utters[-1] == "Вот что я могу:":
            utters.pop()
        if utters:
            new_dialogs.append(utters)
    dialogs = new_dialogs
    dialog_lines = []
    for dialog in dialogs:
        if len(dialog) > 1:
            dialog = drop_repeated_utters(dialog)
            dialog = preproc_dial(dialog)
            dialog_lines.append(list(zip(dialog[::2], dialog[1::2])))
            dialog_lines.append(list(zip(dialog[1::2], dialog[2::2])))
    return dialog_lines


def csv2txt_dialogs(in_csv):
    data = in_csv.open("rt", encoding="utf-16le").readlines()
    dialog_lines = dialog_split(data)
    random.shuffle(dialog_lines)
    return dialog_lines


def collapse_repeating(msgs):
    new_msgs = []
    for msg, next_msg in zip(msgs, msgs[1:]):
        if msg["text"] != next_msg["text"]:
            new_msgs.append(msg)
    if msgs:
        new_msgs.append(msgs[-1])
    return new_msgs


def collapse_user_utterts_serials(msgs):
    utters = []
    side = None
    for msg in msgs:
        if side != msg["side"]:
            utters.append([msg["text"]])
            side = msg["side"]
        else:
            utters[-1].append(msg["text"])

    if utters:
        utters = [" ".join(serial) for serial in utters]
    return utters


def json_msgs_preproc(msgs):
    msgs.sort(key=lambda x: x["id"])

    new_msgs = collapse_repeating(msgs)
    while len(new_msgs) != len(msgs):
        msgs.clear()
        msgs.extend(new_msgs)
        new_msgs = collapse_repeating(msgs)
    msgs.clear()
    msgs.extend(new_msgs)
    utters = collapse_user_utterts_serials(msgs)
    return utters


def json2txt_dialogs(in_json, out_txt):
    out_txt.parent.mkdir(511, True, True)
    js = json.load(in_json.open())
    dialogs = [dialog["messages"] for dialog in js["data"]]
    dialogs = [json_msgs_preproc(dialog) for dialog in dialogs]
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


def get_uniq_dialogs(dialog_lines):
    uniq_dialog_lines = []
    for dialog_line in dialog_lines:
        uniq_dialog_lines.append("|".join(["->".join(uts) for uts in dialog_line]))
    uniq_dialog_lines = set(uniq_dialog_lines)
    dialog_lines = []
    for uniq_dial in uniq_dialog_lines:
        lines = uniq_dial.split("|")
        if lines:
            dialog_lines.append([line.split("->") for line in lines if line])
    return dialog_lines


def write2file(dialog_lines, out_txt):
    out_txt.parent.mkdir(511, True, True)
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
dialog_lines = []
dialog_lines.extend(csv2txt_dialogs(in_csv1))
dialog_lines.extend(csv2txt_dialogs(in_csv2))
dialog_lines.extend(csv2txt_dialogs(in_csv3))
dialog_lines = get_uniq_dialogs(dialog_lines)
write2file(dialog_lines, to_dir / "file")

# json2txt_dialogs(in_json, to_dir / "json_file")

