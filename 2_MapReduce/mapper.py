#!/usr/bin/env python

import sys
import random

for line in sys.stdin:
    print(random.random(), "\t" + line.split('\n')[0])
