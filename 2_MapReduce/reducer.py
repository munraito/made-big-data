#! /usr/bin/env python

import sys
import random

lens = range(1, 6)
i = 0
id_line = ''
curr_len = random.choice(lens)
for line in sys.stdin:
    key, val =  line.split('\t')
    val = val.split('\n')[0]
    if i < curr_len:
        id_line += val + ','
        i += 1
    else:
        print(id_line[:-1])
        i, id_line = 0, ''
        curr_len = random.choice(lens)
