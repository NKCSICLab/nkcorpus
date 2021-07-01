import shutil
import os
import pandas as pd
import numpy as np

df = pd.read_csv('dealt.csv')
paths = df.path.to_numpy()

for path in paths:
    if os.path.exists('/mnt/' + path.replace('original', 'dealt')):
        shutil.move('/mnt/' + path.replace('original', 'dealt'), '/mnt/' + path)
        print(f'Moved: {path}')
