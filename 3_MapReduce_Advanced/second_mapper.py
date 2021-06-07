#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.split('\n')[0]
    year, tag, count = line.split('\t', 2)
    if year in ['2010', '2016']:
        print(year, count, tag, sep='\t')
