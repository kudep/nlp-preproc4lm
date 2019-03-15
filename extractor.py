# %%
import json
import random
import glob
import logging
import re
import pathlib

logFormatter = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

random.seed(315)
valid_part = 0.001

# %%
glob_src = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/src/dialogues_validated_0403.json"
prefix_tgt = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/tgt/clean"
prefix_tgt = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/validated_tgt/clean"
pathlib.Path(prefix_tgt).parent.mkdir(511, True, True)


def reg_apply(text, regs):
    text = str(text)
    for match, to in regs:
        text = match.sub(to, text)
    return text


nl_tag = " "
clean_regexps = [
    (re.compile(r"\s{2,}"), " "),
    (re.compile(r"\{.*\}"), " "),
    (re.compile(r"</*i>"), " "),
    (re.compile(r"\[.*\]"), " "),
    (re.compile(r"…"), r"..."),
    (re.compile(r"[‐‑‒–—―-]{1,}"), "-"),
    (re.compile(r"\r\n-\s*"), nl_tag),
    (re.compile(r"(\r\n\s*)([А-Я])"), nl_tag + r"\g<2>"),
    (re.compile(r"\r\n\s*"), " "),
    (re.compile(r"\n"), nl_tag),
    (re.compile(r"(^\s*-*\s*)([^а-я])"), nl_tag + r"\g<2>"),
    (re.compile(r"([?.!])+\s*[-]\s*"), r"\g<1>" + nl_tag),
    (re.compile(r"<font.*>"), " "),
    (re.compile(r"</?b>"), " "),
    (
        re.compile(
            r'(?i)\b((?:(https?|ftp):\/\/|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}\/)(?:[^\s<>]|\(([^\s<>]+|(\([^\s<>]+\)))*\))+(?:\(([^\s<>]+|(\([^\s<>]+\)))*\)|[^\s`!\[\]{};:\'".,<>?\xab\xbb]))'
        ),
        " ",
    ),
    (re.compile(r"[\(\)]"), ""),
    (re.compile(r"^(\s{0,}[.?!,-]\s{0,})+$"), ""),
    (re.compile(r"\s{2,}"), " "),
    (re.compile(r"^"), " "),
]


def msgs2msgs(msgs):
    msgs = [(reg_apply(utter, clean_regexps), snd) for utter, snd in msgs]
    msgs = [
        (utter.strip(), snd) for i, (utter, snd) in enumerate(msgs) if i in [0, 1] or len(utter.strip().split()) > 1
    ]
    msgs = [(" ".join(utter.split()[-127:]), snd) for (utter, snd) in msgs]
    msgs = [(utter, snd) for (utter, snd) in msgs if utter]
    return msgs


# %%
in_files = glob.glob(glob_src)

# %%
print(in_files)


def merge_utters(dialog):
    sender = None
    utters = []
    for utter in dialog:
        if sender != utter["sender"]:
            sender = utter["sender"]
            utters.append({"sender": utter["sender"], "text": [utter["text"]]})
        else:
            utters[-1]["text"].append(utter["text"])
    for utter in utters:
        utter["text"] = " ".join(utter["text"])
    return utters


def form_struct(sample):
    dialogs = []
    personas = {
        sample["users"][0]["user_id"]: sample["users"][0]["profile"],
        sample["users"][1]["user_id"]: sample["users"][1]["profile"],
    }
    dialog = merge_utters(sample["dialog"])
    msgs = [(utter["text"], utter["sender"]) for utter in dialog]
    msgs = msgs2msgs(msgs)
    utters, senders = list(zip(*msgs))
    if "persona_chat_rus" in "".join(utters):
        return []
    if len(utters) < 4:
        return []
    dialogs.append({"utters": utters[:], "persona": personas[senders[1]]})
    dialogs.append({"utters": utters[1:], "persona": personas[senders[0]]})
    return dialogs


# %%


def json2txt_dialogs(in_files):
    # logger.info(f'Loading from {in_file}')
    with open(prefix_tgt + ".train.txt", "tw") as out_d_train, open(prefix_tgt + ".valid.txt", "tw") as out_d_valid:
        samples = [json.load(open(in_file)) for in_file in in_files]
        samples = sum(samples, [])
        dialogs = []
        for sample in samples:
            dialogs.extend(form_struct(sample))
        random.shuffle(dialogs)
        print(len(dialogs))
        for dialog in dialogs:
            utter_pairs = list(zip(dialog["utters"][::2], dialog["utters"][1::2]))
            persona_offset = len(dialog["persona"]) + 1
            lines = [f"{i} your persona: {persona.strip()}" for i, persona in enumerate(dialog["persona"], 1)]
            dial_lines = [
                f"{i} {utter1.strip()} \t{utter2.strip()}"
                for i, (utter1, utter2) in enumerate(utter_pairs, persona_offset)
            ]
            if not (dial_lines):
                continue
            lines.extend(dial_lines)
            if random.random() <= valid_part:
                out_d_valid.write("\n".join(lines))
                out_d_valid.write("\n")
            else:
                out_d_train.write("\n".join(lines))
                out_d_train.write("\n")


json2txt_dialogs(in_files)
