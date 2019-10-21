#!/usr/bin/env python

import sys
import asyncio
import raft.network as raft

listen_on = tuple(sys.argv[1].split(':'))
nodes = tuple([tuple(x.split(':')) for x in sys.argv[2].split(',')])
asyncio.run(raft.run_server(listen_on, nodes))
