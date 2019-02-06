# %%
import pandas as pd
import random
import argparse
import logging
import tqdm
import traceback
from multiprocessing import Pool
from glob import glob
import pathlib

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
# '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
formatter = logging.Formatter(
    '%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

DATA_DICT = {}


def move_graph(ref_search_id, search_id, utt_graph):
    if ref_search_id in DATA_DICT:
        if 'refs' in DATA_DICT[ref_search_id]:
            refs = []
            for ref in DATA_DICT[ref_search_id]['refs']:
                _refs = move_graph(ref, search_id, utt_graph)
                refs.extend(_refs)

            return refs
        else:
            root_graph = DATA_DICT[ref_search_id]
            root_graph['all_nodes'].update(utt_graph['all_nodes'])
            tgt_graph = root_graph['all_nodes'][search_id]
            tgt_graph['nearby_nodes'][utt_graph['root']['id']] = utt_graph
            return [ref_search_id]
    return []


def utt_graph_walker(utters, graph):
    if graph['root']['quoted_text']:
        utters.append(f"__QUOT__: {graph['root']['quoted_text']}")
    utters.append(f"__UTTER__: {graph['root']['text']}")
    if graph['nearby_nodes']:
        direct_dialogs = []
        for next_graph in graph['nearby_nodes'].values():
            clone_utters = utters.copy()
            direct_dialogs_part = utt_graph_walker(clone_utters, next_graph)
            direct_dialogs.extend(direct_dialogs_part)
        return direct_dialogs
    else:
        return [utters]


def FLAGS(): return None


def timeout_initializer(timeout_duration):
    global FLAGS
    FLAGS.timeout_duration = timeout_duration


def worker(in_file):
    try:
        try:
            df = pd.read_csv(in_file, header=None, engine='python')
        except pd.errors.EmptyDataError:
            logger.warning('Empty file {}'.format(in_file))
            return {}

        df.columns = ['created_at',
                      'id',
                      'text',
                      'quoted_text',
                      'quoted_status_id',
                      'in_reply_to_status_id',
                      ]

        df['quoted_text'] = df['quoted_text'].fillna('')
        df['text'] = df['text'].fillna('')

        df = df.fillna(0)
        df = df[df['id'] != 0]

        fin_data = {}
        for _, row in df.iterrows():
            try:
                id = int(row['id'])
                text = str(row['text'])
                quoted_text = str(row['quoted_text'])
                quoted_status_id = int(row['quoted_status_id'])
                in_reply_to_status_id = int(row['in_reply_to_status_id'])
            except Exception:
                continue
            fin_data[row['id']] = {
                'root': {
                    'id': id,
                    'text': text,
                    'quoted_text': quoted_text,
                    'quoted_status_id': quoted_status_id,
                    'in_reply_to_status_id': in_reply_to_status_id,
                },
                'nearby_nodes': {},
                'all_nodes': {},

            }

            # cicle link
            fin_data[row['id']]['all_nodes'][row['id']] = fin_data[row['id']]
        return fin_data
    except Exception:
        logger.error('Exception in file {}'.format(in_file))
        traceback.print_exc()


def timeouted_worker(inout_file):
    import signal

    class TimeoutError(Exception):
        pass

    def handler(signum, frame):
        raise TimeoutError('Timeout error in file %s' % inout_file[0])
    # set the timeout handler
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(FLAGS.timeout_duration)
    try:
        return worker(inout_file)
    except TimeoutError as exc:
        logger.error(exc)
    finally:
        signal.alarm(0)


def run_pool(files, cpu_n=1):
    # Using initializer and  multi_preprocessing functions from this module
    fin_data = []
    with Pool(cpu_n) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(worker, files), total=len(files)):
            if process_ret:
                fin_data.append(process_ret)
    return fin_data


def timeouted_run_pool(files, cpu_n=1, timeout_duration=40*60):
    # Using initializer and  multi_preprocessing functions from this module
    fin_data = []
    with Pool(cpu_n, timeout_initializer, initargs=[timeout_duration]) as p:
        for process_ret in tqdm.tqdm(p.imap_unordered(timeouted_worker, files), total=len(files)):
            if process_ret:
                fin_data.append(process_ret)
    return fin_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--from_files_pattern', default='/home/denis/tmp/df-*', type=str)
    parser.add_argument('-t', '--to_file', default='/home/denis/tmp/dialogs.txt', type=str)
    parser.add_argument('-c', '--chunk_size', default=10000, type=int)
    parser.add_argument('-n', '--cpu_n', default=5, type=int)
    parser.add_argument('-T', '--timeout_duration', default=60*60, type=int)  # timeout_duration = 1 hours
    args = parser.parse_args()

    from_files = glob(args.from_files_pattern)
    from_files = [str(pathlib.Path(file).resolve()) for file in from_files]

    logger.info('Loading from csv files.')
    logger.info('One csv file to one utterence nodes dict.')
    nodes_dicts = timeouted_run_pool(from_files, cpu_n=args.cpu_n,
                                     timeout_duration=args.timeout_duration)
    logger.info('Aggregate dicts of utterence nodes to one share nodes dict.')
    for nodes_dict in tqdm.tqdm(nodes_dicts):
        DATA_DICT.update(nodes_dict)
    del nodes_dicts

    logger.info('Rebuilding nodes dict to dialogs graphs dict.')
    for k in tqdm.tqdm(DATA_DICT.keys(), total=len(DATA_DICT)):
        # Get graph of utterances
        utt_graph = DATA_DICT[k]
        # Check existing of utterances
        if 'refs' in utt_graph:
            # if it is pointer to another utterance then skip
            continue
        refs = []

        # for id_type_name in ['in_reply_to_status_id', 'quoted_status_id']:
        search_id = utt_graph['root']['in_reply_to_status_id']
        if search_id == 0:
            continue
        displaced_refs = move_graph(search_id, search_id, utt_graph)
        refs.extend(displaced_refs)
        if refs:
            DATA_DICT[k] = {
                'refs': refs,
            }

    logger.info('Dropping useless elements of graphs dict.')
    part_size = args.chunk_size
    drop_keys_batches = []
    for i, k in tqdm.tqdm(enumerate(DATA_DICT.keys()), total=len(DATA_DICT)):
        indx = i//part_size
        utt_graph = DATA_DICT[k]
        if len(drop_keys_batches) <= indx:
            drop_keys_batches.append([])
        if not (utt_graph.get('nearby_nodes')):
            drop_keys_batches[indx].append(k)
        elif 'refs' in utt_graph:
            drop_keys_batches[indx].append(k)

    for drop_batch in tqdm.tqdm(drop_keys_batches):
        for key in drop_batch:
            del DATA_DICT[key]

    logger.info('Creating list of separated dialogs.')
    part_size = args.chunk_size
    direct_dialog_parts = []
    for i, graph in tqdm.tqdm(enumerate(DATA_DICT.values()), total=len(DATA_DICT)):
        indx = i//part_size
        if len(direct_dialog_parts) <= indx:
            direct_dialog_parts.append([])
        utters = []
        direct_dialogs = utt_graph_walker(utters, graph)
        direct_dialog_parts[indx].extend(direct_dialogs)

    logger.info('Dumping list of separated dialogs to file.')
    with open(args.to_file, 'wt') as f:
        random.shuffle(direct_dialog_parts)
        for dialogs_sample in tqdm.tqdm(direct_dialog_parts, total=len(direct_dialog_parts)):
            random.shuffle(dialogs_sample)
            for dialog_utters in dialogs_sample:
                f.write('\n'.join(dialog_utters))
                f.write('\n__NEXT_SAMPLE__\n')


if __name__ == '__main__':
    main()
