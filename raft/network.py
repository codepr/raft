import asyncio


class RaftServerProtocol(asyncio.Protocol):

    def __init__(self, timeout, nodes=()):
        self.timeout = timeout
        self.nodes = nodes
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

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: RaftServerProtocol(timeout, nodes),
        local_addr=addr,
        reuse_address=True
    )

    await asyncio.sleep(60)


class RaftClientProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        self.transport = transport
        print('Connection made')

    def datagram_received(self, data, addr):
        print('Data received from {}: {!r}'.format(addr, data.decode()))

    def error_received(self, exc):
        print('Received an error')


async def get_client(self, addr=('127.0.0.1', 20001)):
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: RaftClientProtocol(),
        remote_addr=addr,
        reuse_address=True
    )
    return transport
