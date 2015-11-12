#!/usr/bin/env python

import sys
import fileinput
import re
import pymongo

log_re = re.compile(
    r'^(\d+\.\d+\.\d+\.\d+) - - '
    r'\[(.+)\] '
    r'"(\w+) (\S+) \S+" '
    r'(\d+) \d+ '
    r'"(\S+|-)" '
    r'"(.+)" '
    r'"(\S+|-)"$'
)

url = sys.argv[1]
db = sys.argv[2]
log_path = sys.argv[3]

client = pymongo.MongoClient('mongodb://' + url)

for line in fileinput.input(log_path):
    match = log_re.match(line[:-1])
    if match:
        groups = match.groups()
        remote_addr = groups[0]
        timestamp = groups[1]
        method = groups[2]
        path = groups[3]
        status = groups[4]
        refer = groups[5]
        agent = groups[6]

        client[db].access.insert({
                'remote_addr': remote_addr,
                'timestamp': timestamp,
                'method': method,
                'path': path,
                'status': status,
                'refer': refer,
                'agent': agent,
        })
