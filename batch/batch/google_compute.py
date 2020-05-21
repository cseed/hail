import asyncio
import datetime
import concurrent
import logging

import google.cloud.logging

from .batch_configuration import PROJECT

log = logging.getLogger('google_compute')


async def anext(ait):
    return await ait.__anext__()


class EntryIterator:
    def __init__(self, gservices, db):
        self.gservices = gservices
        self.db = db
        self.mark = None
        self.entries = None

    async def async_init(self):
        row = await self.db.select_and_fetchone('SELECT * FROM `gevents_mark`;')
        if row['mark']:
            self.mark = row['mark']
        else:
            now = datetime.datetime.utcnow().isoformat() + 'Z'
            await self._update_mark(now)

    async def _update_mark(self, timestamp):
        await self.db.execute_update(
            'UPDATE `gevents_mark` SET mark = %s;',
            (timestamp,))
        self.mark = timestamp

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if not self.entries:
                await asyncio.sleep(5)
                self.entries = await self.gservices.list_entries(self.mark)
            timestamp = None
            try:
                entry = await anext(self.entries)
                timestamp = entry.timestamp.isoformat()
                return entry
            except StopAsyncIteration:
                self.entries = None
            finally:
                if timestamp:
                    await self._update_mark(timestamp)


class PagedIterator:
    def __init__(self, gservices, pages):
        self.gservices = gservices
        self.pages = pages
        self.page = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self.page is None:
                await asyncio.sleep(5)
                try:
                    self.page = next(self.pages)
                except StopIteration:
                    raise StopAsyncIteration
            try:
                return next(self.page)
            except StopIteration:
                self.page = None


class GServices:
    def __init__(self, machine_name_prefix, credentials):
        self.machine_name_prefix = machine_name_prefix
        self.logging_client = google.cloud.logging.Client(credentials=credentials)
        self.loop = asyncio.get_event_loop()
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=40)
        self.credentials = credentials

        self.filter = f'''
logName="projects/{PROJECT}/logs/compute.googleapis.com%2Factivity_log" AND
resource.type=gce_instance AND
jsonPayload.resource.name:"{self.machine_name_prefix}" AND
jsonPayload.event_subtype=("compute.instances.preempted" OR "compute.instances.delete")
'''
        log.info(f'filter {self.filter}')

    async def run_in_pool(self, f, *args, **kwargs):
        return await self.loop.run_in_executor(self.thread_pool, lambda: f(*args, **kwargs))

    # logging
    async def list_entries(self, timestamp):
        filter = self.filter + f' AND timestamp >= "{timestamp}"'
        entries = self.logging_client.list_entries(filter_=filter, order_by=google.cloud.logging.ASCENDING)
        return PagedIterator(self, entries.pages)

    async def stream_entries(self, db):
        eit = EntryIterator(self, db)
        await eit.async_init()
        return eit