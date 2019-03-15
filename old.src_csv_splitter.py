
import pandas as pd
import random
import tqdm
import logging
logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def chunk_generator(items_list, chunk_size):
    for i in range(0, len(items_list), chunk_size):
        yield items_list[i:i + chunk_size]


random.seed(315)
path_src = '/home/den/Documents/chit-chat_2019/data/sber_srt/d_srt/data/src/srt.csv'
part_size = 1000
logger.info(f'Loading from {path_src}')
df = pd.read_csv(path_src,  sep=';')
logger.info(f'Loading is finished')

logger.info(f'Splitting by slices.')
df1 = df['ind_film']
df2 = df1.drop_duplicates(keep='first')
slices = list(zip(df2.index[:-1], df2.index[1:]-1))
random.shuffle(slices)
logger.info(f'Splitting is finished')

logger.info(f'Saving to files')
for i, slises_shard in enumerate(tqdm.tqdm(chunk_generator(slices, part_size), total=len(slices)//part_size)):
    tmp_df = pd.concat([df.loc[start:end] for start, end in slises_shard])
    tmp_df.to_csv(f'/home/den/Documents/chit-chat_2019/data/sber_srt/d_srt/data/tgt/src_parts/src_part_{i:03}.csv',
                  sep=';',
                  index=False,
                  )
