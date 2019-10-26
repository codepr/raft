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


async def start_server(listen_on, nodes):
    loop = asyncio.get_running_loop()
    event_queue = asyncio.Queue()
    task = loop.create_task(raft.run_server(event_queue, listen_on, nodes))
    try:
        while True:
            await asyncio.sleep(5)
            await event_queue.put({'cmd': 'SET X 5'})
    finally:
        task.cancel()


def run(listen_on, nodes):
    asyncio.run(start_server(listen_on, nodes))


proc1 = mp.Process(target=run, args=(listen_on, nodes), daemon=True)
proc2 = mp.Process(target=run, args=(nodes[0], (listen_on,)), daemon=True)

proc1.start()
proc2.start()

mp.Event().wait()
