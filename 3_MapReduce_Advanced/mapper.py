#!/usr/bin/env python3

import sys
import xml.etree.ElementTree as et
# from collections import Counter

for line in sys.stdin:
    try:
        xml = et.fromstring(line.strip())
    except:
        continue
    if xml.tag == 'row':
        dt = xml.get('CreationDate')
        if dt:
            year = dt[:4]
        tags = xml.get('Tags')
        if tags:
            tags = tags.replace('<',' ').replace('>',' ').split()
        # counts = Counter(tags)
        # for tag, tag_count in counts.items():
            for tag in tags:
                if year and tag:
                    print(year, tag, 1, sep = '\t')
