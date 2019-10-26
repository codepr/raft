#!/usr/bin/env python

import sys
import asyncio
import multiprocessing as mp
import raft.network as raft


def to_addr(arg):
    host, port = arg.split(':')
    return host, int(port)


listen_on = to_addr(sys.argv[1])
nodes = tuple([to_addr(x) for x in sys.argv[2].split(',')])


def run(listen_on, nodes):
    asyncio.run(raft.run_server(asyncio.Queue(), listen_on, nodes))


proc1 = mp.Process(target=run, args=(listen_on, nodes), daemon=True)
proc2 = mp.Process(target=run, args=(nodes[0], (listen_on,)), daemon=True)

proc1.start()
proc2.start()

mp.Event().wait()
