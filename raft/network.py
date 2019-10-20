import asyncio


class RaftServerProtocol(asyncio.Protocol):

    def __init__(self, timeout, nodes=()):
        self.timeout = timeout
        self.nodes = nodes
        loop = asyncio.get_running_loop()
        self.heartbeat = loop.create_task(self.send_heartbeat())
        self.client = loop.create_task(self.get_client(nodes))

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()
        print('Data received from {}: {!r}'.format(addr, message))

    def send_data(self, data):
        self.transport.sendto(data)

    def close(self):
        self.transport.close()

    async def get_client(self, addr=('127.0.0.1', 20001)):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: RaftClientProtocol(),
            remote_addr=addr,
            reuse_address=True
        )
        return transport

    async def send_heartbeat(self):
        client = await self.client
        while True:
            # TODO send heartbeat
            client.sendto(b"BEAT")
            await asyncio.sleep(self.timeout)


async def run_server(addr=('127.0.0.1', 20000), nodes=(), timeout=1):
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

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
