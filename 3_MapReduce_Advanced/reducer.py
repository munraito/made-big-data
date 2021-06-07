#!/usr/bin/env python3

import sys

curr_year, curr_tag, tag_count = None, None, 0

for line in sys.stdin:
    year, tag, count =  line.split('\t', 2)
    count = int(count)
    if tag == curr_tag and year == curr_year:
        tag_count += count
    else:
        if curr_tag and curr_year:
            print(curr_year, curr_tag, tag_count, sep='\t')
        curr_year, curr_tag, tag_count = year, tag, count
if curr_tag:
    print(curr_year, curr_tag, tag_count, sep='\t')
