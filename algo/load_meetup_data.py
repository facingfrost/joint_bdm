import os 
import pandas as pd
from subprocess import PIPE, Popen
import time

timestr = time.strftime("%Y%m%d")

meetup = pd.read_json(f'./data/meetup-{timestr}.json')
meetup.to_orc(f'./data/meetup-{timestr}.orc')

file_name = f'./data/meetup-{timestr}.orc'
hdfs_path = os.path.join(os.sep, 'user', 'shofiyyahnadhiroh', file_name)

put = Popen(["hadoop", "fs", "-put", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
put.communicate()