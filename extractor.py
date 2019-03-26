# %%
import json
import random
import glob
import logging
import re
import pathlib
import yandex_spellcheck_api
import tqdm
import difflib

# import pprint
from nltk import word_tokenize
from mosestokenizer import MosesDetokenizer
from multiprocessing import Pool


from deeppavlov import configs
from deeppavlov import build_model

logFormatter = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

SEP_TOKEN = " \n "
MAX_CHAR_LEN = 7000
SHUFFLE = True
VALID_PART = 0.001
SEED = 31514
WORKER_NUM = 10

ner = build_model(config=configs.ner.ner_rus_lower_vpc_with_context, download=True)

detokenizer = MosesDetokenizer()

random.seed(SEED)

# %%
glob_src = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/src/valid_dialogues_10K.json"
spellchecked_src = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/spellchecked_src/"
prefix_tgt = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/tgt/clean"
prefix_tgt = "/home/den/Documents/chit-chat_2019/data/toloka_dialogues/validated_tgt_2019_03_20/clean"
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
    # (re.compile(r"[\(\)]"), ""),
    # (re.compile(r"^(\s{0,}[.?!,-]\s{0,})+$"), ""),
    # (re.compile(r"\s{2,}"), " "),
    # (re.compile(r"^"), " "),
]


def msgs2msgs(msgs):
    # pprint.pprint(msgs)
    # pprint.pprint(len(msgs))
    msgs = [(reg_apply(utter, clean_regexps), snd) for utter, snd in msgs]
    msgs = [
        (utter.strip(), snd) for i, (utter, snd) in enumerate(msgs) if i in [0, 1] or len(utter.strip().split()) > 0
    ]
    msgs = [(" ".join(utter.split()[-127:]), snd) for (utter, snd) in msgs]
    msgs = [(utter, snd) for (utter, snd) in msgs if utter]
    # pprint.pprint("--------------------------------------------------------------------------")
    # pprint.pprint(msgs)
    # pprint.pprint(len(msgs))
    # pprint.pprint("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    return msgs


# %%
def chunk_generator(items_list, chunk_size):
    for i in range(0, len(items_list), chunk_size):
        yield items_list[i : i + chunk_size]


def smart_merge_shards(shards, merge_token_len, max_char_len):  # 10000 max_char_len
    # if len(shards) == 2:
    #     lens = [sh["len"] for sh in shards]
    #     shard_len = sum(lens, merge_token_len)
    #     if shard_len < max_char_len:
    #         shards = [{"indexes": shards[0]["indexes"] + shards[1]["indexes"], "len": shard_len}]
    #         return shards
    return shards


def create_text_shards(utters):
    shards = [{"indexes": [i], "len": len(text)} for i, text in enumerate(utters)]
    full_merge_flag = [True]
    res_shards = []
    while sum(full_merge_flag, 0) and len(shards) > 1:
        full_merge_flag.clear()
        new_shards = []
        for shard_pair in chunk_generator(shards, 2):
            merged_shards = smart_merge_shards(shard_pair, len(SEP_TOKEN), MAX_CHAR_LEN)
            full_merge_flag.append(len(shard_pair) != len(merged_shards))
            new_shards.extend(merged_shards)
        shards = new_shards
        new_shards = []
        for shard_pair in chunk_generator(shards[1:], 2):
            merged_shards = smart_merge_shards(shard_pair, len(SEP_TOKEN), MAX_CHAR_LEN)
            full_merge_flag.append(len(shard_pair) != len(merged_shards))
            new_shards.extend(merged_shards)
        shards = shards[:1] + new_shards
        res_shards.clear()
        res_shards.extend(shards)
    text_shards = [SEP_TOKEN.join([str(utters[index]) for index in shard["indexes"]]) for shard in shards]
    return text_shards


def text_shards_spellcheck(text_shards):
    rets = []
    with Pool(WORKER_NUM) as p:
        for ret in tqdm.tqdm(p.imap(yandex_spellcheck_api.send, text_shards), total=len(text_shards)):
            rets.append(ret)
    pairs = [(i, j) for i, j in list(zip(text_shards, rets)) if i != j]
    # pprint.pprint(pairs)
    return rets


def text_shards2utters(text_shards):
    utters = []
    for txt in text_shards:
        utters.extend([t.strip() for t in txt.split(SEP_TOKEN[1:-1])])
    return utters


def patches_apply(utter):
    # utter = utter.replace("Феникс", "И тебе удачи!")
    # utter = utter.replace("ём занимаешься", "Чем занимаешься?")
    return utter


def clean_utters(utters):
    utters = [detokenizer(word_tokenize(ut)) for ut in utters]
    # fix patches
    utters = [patches_apply(ut) for ut in utters]
    return utters


# %%

exceptions_vocab = [
    "Ооо".lower(),
    "Спс".lower(),
    "Ясненько".lower(),
    "Щас".lower(),
    "Ха".lower(),
    "Ахахаха".lower(),
    "Ку".lower(),
    "Шалом".lower(),
    "Бай".lower(),
    "Ладненько".lower(),
    "АяА".lower(),
    "Кхм".lower(),
    "Во".lower(),
    "Жалко".lower(),
    "ЯЯ".lower(),
    "Вискарь".lower(),
    "Йога".lower(),
    "Хо".lower(),
    "Вулдеркинд".lower(),
    "Лошара".lower(),
    "Пей".lower(),
    "Лошара".lower(),
    "НуНу".lower(),
    "Клоун".lower(),
    "ЛадненькоЛадненько".lower(),
    "Бабушкины".lower(),
    "Хе".lower(),
    "В".lower(),
    "Ай".lower(),
    "Фантастический".lower(),
    "Дайвинг".lower(),
    "Рутина".lower(),
    "Папина".lower(),
    "Сова".lower(),
    "Ай".lower(),
    "Оу".lower(),
    "Комбайнером".lower(),
    "Сорян".lower(),
    "Кайф".lower(),
    "Универ".lower(),
    "Доров".lower(),
    "Молодчина".lower(),
    "ХехехеХе".lower(),
    "Хай".lower(),
    "Сидор".lower(),
    "Кары".lower(),
    "Харе".lower(),
    "ЛадЛадно".lower(),
    "Пивко".lower(),
    "Милашка😄".lower(),
    "Нихао".lower(),
    "Дарова".lower(),
    "ШШотландка".lower(),
    "Канеш".lower(),
]

# %%


def gluing_utters(utters, context):
    new_utters = []
    for utter in utters:
        src = utter[1:]
        lower = src.lower()
        ratio = difflib.SequenceMatcher(a=src, b=lower).ratio()
        if (ratio >= 0.75 and len(utter) < 10) or (len(utter) > 10 and ratio >= 0.90):
            new_utters.append(utter)
        else:
            new_utters.append(utter.lower())
    utters = new_utters

    text_shards = []
    for utter, next_utter in zip(utters, utters[1:] + ["<EOM>"]):
        utter = utter.strip()
        next_utter = next_utter.strip()
        if not (utter):
            continue
        text_shards.append(utter)
        if next_utter == "<EOM>":
            text_shards.append(" ")
        elif utter[-1] in ",.!?:;-()[]":
            text_shards.append(" ")
        elif next_utter:
            if next_utter[0].isupper():
                context_len = None if context else 0
                context = context if context else "."
                try:
                    in_data, out_data = ner([context, context + " " + next_utter + ". "])
                except Exception:
                    print([context, context + " " + next_utter + ". "])
                # print(ner([context, context + " " + next_utter + ". "]))
                context_len = context_len if context_len is not None else len(in_data[0])
                token = in_data[1][context_len:][0]
                tag = out_data[1][context_len:][0]
                if tag in ["B-PER", "B-LOC"] and not (token.lower() in exceptions_vocab):
                    text_shards.append(" ")
                else:
                    text_shards.append(". ")
            elif next_utter[0].isnumeric():
                text_shards.append(" ")
            else:
                text_shards.append(", ")
    return "".join(text_shards)


def merge_utters(dialog):
    sender = None
    utters = []
    for utter in dialog:
        if sender != utter["sender"]:
            sender = utter["sender"]
            utters.append({"sender": utter["sender"], "text": [utter["text"]]})
        else:
            utters[-1]["text"].append(utter["text"])
    context = ""
    for utter in utters:
        utter["text"] = gluing_utters(utter["text"], context)
        context += utter["text"] + ". "
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


def utters_extarct_from_dialogs(dialogs):
    utters = []
    [[utters.append(msg["text"]) for msg in dialog["dialog"]] for dialog in dialogs]
    utters = [ut.replace("\n", " ") for ut in utters]
    return utters


def utters_import_to_dialogs(utters, dialogs):
    index = 0
    new_dialogs = []
    for dialog in dialogs:
        for msg in dialog["dialog"]:
            msg["text"] = utters[index]
            index += 1
            if index >= len(utters):
                new_dialogs.append(dialog)
                return new_dialogs
        new_dialogs.append(dialog)
    return new_dialogs


def spellchecking_files(glob_src, spellchecked_src):
    in_files = glob.glob(glob_src)
    in_files = [pathlib.Path(f) for f in in_files]
    spellchecked_src = pathlib.Path(spellchecked_src)
    spellchecked_src.mkdir(511, True, True)
    out_files = [spellchecked_src / f.name for f in in_files]
    for in_file, out_file in zip(in_files, out_files):
        dialogs = json.load(in_file.open())
        utters = utters_extarct_from_dialogs(dialogs)
        logger.info("create_text_shards")
        text_shards = create_text_shards(utters)
        logger.info("text_shards_spellcheck")
        text_shards = text_shards_spellcheck(text_shards)
        logger.info("text_shards2utters")
        utters = text_shards2utters(text_shards)
        utters = clean_utters(utters)
        logger.info("utters_import_to_dialogs")
        dialogs = utters_import_to_dialogs(utters, dialogs)
        json.dump(dialogs, out_file.open("wt", encoding="utf-8"), ensure_ascii=False)


def json2txt_dialogs(in_files):
    # logger.info(f'Loading from {in_file}')
    with open(prefix_tgt + ".train.txt", "tw") as out_d_train, open(prefix_tgt + ".valid.txt", "tw") as out_d_valid:
        samples = [json.load(open(in_file)) for in_file in in_files]
        samples = sum(samples, [])
        dialogs = []
        print(len(samples))
        for sample in tqdm.tqdm(samples):
            dialogs.extend(form_struct(sample))
        if SHUFFLE:
            random.shuffle(dialogs)
        for dialog in dialogs:
            utter_pairs = list(zip(dialog["utters"][::2], dialog["utters"][1::2]))
            persona_offset = len(dialog["persona"]) + 1
            lines = [f"{i} your persona: {persona.strip().lower()}" for i, persona in enumerate(dialog["persona"], 1)]
            dial_lines = [
                f"{i} {utter1.strip()} \t{utter2.strip()}"
                for i, (utter1, utter2) in enumerate(utter_pairs, persona_offset)
            ]
            if not (dial_lines):
                continue
            lines.extend(dial_lines)
            if random.random() <= VALID_PART:
                out_d_valid.write("\n".join(lines))
                out_d_valid.write("\n")
            else:
                out_d_train.write("\n".join(lines))
                out_d_train.write("\n")


# spellchecking_files(glob_src, spellchecked_src)
# # %%
in_files = glob.glob(spellchecked_src + "/valid_dialogues_10K.json")

json2txt_dialogs(in_files)
