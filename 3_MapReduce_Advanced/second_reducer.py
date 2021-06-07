#!/usr/bin/env python3

import sys

cnt_2010, cnt_2016 = 0, 0

for line in sys.stdin:
    line = line.split('\n')[0]
    year, count, tag =  line.split('\t', 2)
    if year == '2010' and cnt_2010 < 10:
        print(year, tag, count, sep='\t')
        cnt_2010 += 1
    elif year == '2016' and cnt_2016 < 10:
        print(year, tag, count, sep='\t')
        cnt_2016 += 1
#    if cnt_2010 == 10 and cnt_2016 == 10:
#        break;
