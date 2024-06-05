import os 
import pandas as pd
from subprocess import PIPE, Popen
import time

timestr = time.strftime("%Y%m%d")

post = pd.read_json(f'./data/post-{timestr}.json')
post.to_orc(f'./data/post-{timestr}.orc')

file_name = f'./data/post-{timestr}.orc'
hdfs_path = os.path.join(os.sep, 'user', 'shofiyyahnadhiroh', file_name)

put = Popen(["hadoop", "fs", "-put", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
put.communicate()