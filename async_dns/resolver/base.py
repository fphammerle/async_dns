import asyncio
from async_dns.core import CacheNode, get_root_servers, logger, Address, DNSMessage, REQUEST, Record, types
from .request import tcp, udp, doh


class BaseResolver:
    '''
    Resolve a name by requesting a remote name server.
    '''

    protocols = {
        'tcp': tcp.request,
        'tcps': tcp.request,
        'udp': udp.request,
        'https': doh.request,
    }

    def __init__(self, timeout=5.0):
        self.request_cache = {}
        self.timeout = timeout

    async def query(self, fqdn: str, qtype: int, addr: Address):
        '''
        Query a name from a remote name server.

        It is guarenteed a query will only be sent once before it gets a response or timeout error.
        '''
        key = fqdn, qtype, addr
        task = self.request_cache.get(key)
        if task is None:
            task = asyncio.ensure_future(self._query(fqdn, qtype, addr))
            task.add_done_callback(lambda x: self.request_cache.pop(key, None))
            self.request_cache[key] = task
        return await task

    async def _query(self, fqdn: str, qtype: int, addr: Address):
        req = DNSMessage(qr=REQUEST)
        req.qd.append(Record(REQUEST, fqdn, qtype))
        logger.debug('[BaseResolver:query][%s][%s] %s', types.get_name(qtype),
                     fqdn, addr)
        res = await asyncio.wait_for(self._request(req, addr), self.timeout)
        return res

    async def _request(self, req, addr):
        '''Return response to a request.

        Send DNS request data with `protocol`.
        '''
        request = self.protocols[addr.protocol]
        data = await request(req, addr, self.timeout)
        return data


if __name__ == '__main__':

    async def main():
        resolver = BaseResolver()
        res = await resolver.query('www.baidu.com', types.A,
                                   Address.parse('tcp://192.168.31.1'))
        from .request import clean
        clean()
        print(res)

    asyncio.run(main())