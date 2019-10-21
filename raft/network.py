import asyncio


class RaftServerProtocol(asyncio.DatagramProtocol):

    def __init__(self, timeout, on_con_lost, nodes=()):
        self.timeout = timeout
        self.nodes = nodes
        self.on_con_lost = on_con_lost
        self.loop = asyncio.get_running_loop()
        self.nodes = nodes

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport
        self.send_heartbeat()

    def datagram_received(self, data, addr):
        message = data.decode()
        print('Data received from {}: {!r}'.format(addr, message))

    def error_received(self, exc):
        print('Received an error', exc)

    def connection_lost(self, exc):
        print('Connection closed')
        self.on_con_lost.set_result(True)

    def send_heartbeat(self):
        # TODO send heartbeat
        for node in self.nodes:
            node.sendto(b"BEAT")
        self.loop.call_later(self.timeout, self.send_heartbeat)


async def run_server(addr=('127.0.0.1', 20000), nodes_addrs=(), timeout=5):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    nodes = await asyncio.gather(
        *[get_client(node_addr) for node_addr in nodes_addrs],
        loop=loop
    )

    on_con_lost = loop.create_future()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftServerProtocol(timeout, on_con_lost, nodes),
        local_addr=addr
    )

    try:
        await on_con_lost
    finally:
        transport.close()


class RaftClientProtocol(asyncio.DatagramProtocol):

    def connection_made(self, transport):
        self.transport = transport
        print('Connection made')

    def datagram_received(self, data, addr):
        print('Data received from {}: {!r}'.format(addr, data.decode()))

    def error_received(self, exc):
        print('Received an error', exc)


async def get_client(addr=('127.0.0.1', 20001)):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftClientProtocol(),
        remote_addr=addr
    )
    return transport
