import asyncio
from . import tcp, udp
from async_dns.core import TCP, DNSError, DNSMessage, NameServers, logger, types, Address, REQUEST, Record, InvalidNameServer

A_TYPES = types.A, types.AAAA
PENDING = 0
RESOLVED = 1
REJECTED = 2

class Query:
    def __init__(self, resolver, loop, fqdn, qtype):
        self.loop = loop
        self.resolver = resolver
        self.fqdn = fqdn
        self.qtype = qtype
        self.future = loop.create_future()
        self.from_cache = False
        self._status = PENDING
        self._result = DNSMessage(ra=resolver.recursive)
        self._result.qd.append(Record(REQUEST, name=fqdn, qtype=qtype))

    async def query(self):
        error = None
        domain = self.fqdn
        nameservers = None
        try:
            while True:
                if not self.future.cancelled():
                    self.from_cache = await self.query_cache(domain)
                    logger.debug('[query_cache][%s][%s] %s', types.get_name(self.qtype), domain, self.from_cache)
                    if self.from_cache: break
                if not self.future.cancelled():
                    remote_res = await self.query_remote(domain, nameservers)
                    if remote_res is None: break
                    domain, nameservers = remote_res
        except Exception as e:
            error = e
        if not self.future.cancelled():
            if error:
                self.future.set_exception(error)
            else:
                self.future.set_result((self._result, self.from_cache))

    async def query_cache(self, domain):
        '''Returns a boolean whether a cache hit occurs.'''
        resolver = self.resolver
        cache = resolver.cache
        result = self._result
        # if cached CNAME
        cname = list(cache.query(domain, types.CNAME))
        if cname:
            result.an.extend(rec.copy(name=domain) for rec in cname)
            if all(rec.ttl < 0 for rec in cname):
                result.aa = 1
            if self.qtype == types.CNAME:
                return True
            for rec in cname:
                inter_res = await resolver.query(rec.data, self.qtype)
                if inter_res is None or inter_res.r > 0:
                    continue
                result.an.extend(inter_res.an)
                result.ns = inter_res.ns
                result.ar = inter_res.ar
            return True
        # else
        cache_hit = False
        for rec in cache.query(domain, self.qtype):
            if rec.qtype in (types.NS,):
                inter_res = list(cache.query(rec.data, A_TYPES))
                if inter_res:
                    result.ar.extend(inter_res)
                    result.ns.append(rec)
                    if rec.qtype == self.qtype:
                        cache_hit = True
            else:
                result.an.append(rec.copy(name=domain))
                if self.qtype == types.CNAME or rec.qtype != types.CNAME:
                    cache_hit = True
        if any(domain.endswith(root) for root in self.resolver.rootdomains):
            if not cache_hit:
                # not exists
                result.r = 3
                cache_hit = True
            # should only be added for domains that are resolved by this server
            result.aa = 1  # Authoritative answer
            # result.ns.append(Record(name=domain, qtype=types.NS, data='localhost', ttl=-1))
            # result.ar.append(Record(name=domain, qtype=types.A, data='127.0.0.1', ttl=-1))
        return cache_hit

    async def query_remote(self, domain, nameservers):
        '''Query domain from remote servers.'''
        resolver = self.resolver
        result = self._result
        has_result = False
        inter_res = await self.query_remote_once(domain, nameservers)
        cname = []
        has_ns = False
        for rec in inter_res.an:
            result.an.append(rec)
            if rec.qtype == types.CNAME:
                cname.append(rec.data)
            if self.qtype == types.CNAME or rec.qtype != types.CNAME:
                has_result = True
        for rec in inter_res.ns:
            if not resolver.recursive:
                result.ns.append(rec)
            if rec.qtype == types.SOA or self.qtype == types.NS:
                has_result = True
            else:
                has_ns = True
        if not resolver.recursive:
            result.ar.extend(inter_res.ar)
        if has_result:
            return
        if cname:
            return cname[0], None
        if not resolver.recursive:
            result.r = inter_res.r
            return
        if not has_ns:
            result.r = 2
            return
        nsip_map = {}
        for rec in inter_res.ar:
            nsip_map[rec.name, rec.qtype] = rec.data
        nsips = []
        for rec in inter_res.ns:
            host = rec.data.mname if rec.qtype == types.SOA else rec.data
            ip = nsip_map.get((host, types.A))
            if ip is not None:
                nsips.append(ip)
        return domain, NameServers(nsips)

    async def query_remote_once(self, domain, nameservers=None):
        resolver = self.resolver
        req = DNSMessage(qr=REQUEST)
        if nameservers is None:
            nameservers = resolver.get_nameservers(domain)
        req.qd = [Record(REQUEST, domain, self.qtype)]
        logger.debug('[query_remote][%s][%s] %s', types.get_name(self.qtype), domain, nameservers)
        inter_res = await self.request_remote(nameservers, req)
        resolver.cache_message(inter_res)
        return inter_res

    async def request_remote(self, nameservers, req):
        while not self.future.cancelled():
            addr = nameservers.get()
            if not addr:
                raise InvalidNameServer
            try:
                data = await self.request_once(req, addr)
                inter_res = DNSMessage.parse(data)
                logger.debug('[request_remote] %s', inter_res)
                if inter_res.qd[0].name != req.qd[0].name:
                    raise DNSError(-1, 'Question section mismatch')
                assert inter_res.r != 2, 'Remote server fail'
            except (asyncio.TimeoutError, AssertionError) as e:
                logger.debug('[request_remote][server_error] %s', e)
                nameservers.fail(addr)
            except DNSError:
                logger.debug('[request_remote][dns_error] %s', e)
            else:
                return inter_res

    async def request_once(self, req, addr, protocol=None):
        '''Return response to a request.

        Send DNS request data with `protocol`.
        '''
        if protocol is None:
            protocol = self.resolver.protocol
        if protocol is TCP:
            request = tcp.request
        else:
            request = udp.request
        data = await request(req, addr, self.resolver.request_timeout)
        return data
