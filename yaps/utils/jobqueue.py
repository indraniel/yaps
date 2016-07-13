import os, sqlite3, time

# for future threading considerations...punting for now...
from dummy_thread import get_ident

from yaps.utils.scheduler import bsub

class DrmaaJobQueue(object):
    __create = (
        'CREATE TABLE IF NOT EXISTS queue '
        '( id INTEGER PRIMARY KEY AUTOINCREMENT, jobId INTEGER )'
    )

    __count      = 'SELECT COUNT(*) from queue'
    __iterate    = 'SELECT id, jobId FROM queue ORDER BY jobId'
    __append     = 'INSERT INTO QUEUE (jobId) VALUES (?)'
    __write_lock = 'BEGIN IMMEDIATE'
    __jobs        = 'SELECT jobId FROM queue ORDER BY jobId'
    __clear      = 'DELETE FROM queue'
    __vacuum     = 'VACUUM'

    def __init__(self, path, logger):
        self.path = os.path.abspath(path)
        self.log = logger
        self._connection_cache = {}
        with self._get_db_connection() as c:
            c.execute(self.__create)

    def __len__(self):
        count = 0
        with self._get_db_connection() as c:
            count = c.execute(self.__count).next()[0]
        return count

    def __iter__(self):
        with self._get_db_connection() as c:
            for id, job_id in c.execute(self.__iterate):
                yield job_id

    def _get_db_connection(self):
        id = get_ident()
        if id not in self._connection_cache:
            self._connection_cache[id] = sqlite3.Connection(self.path, timeout=60)
        return self._connection_cache[id]

    def append(self, job_id):
        with self._get_db_connection() as c:
            c.execute(self.__write_lock)
            c.execute(self.__append, (job_id,))
            c.commit() # unlock the database

    def jobs(self):
        with self._get_db_connection() as c:
            cursor = c.execute(self.__jobs)
            job_ids = [ row[0] for row in cursor.fetchall() ]
        return job_ids

    def clear(self):
        self.log.info("Clearing out the LSF job DB")
        with self._get_db_connection() as c:
            c.execute(self.__write_lock)
            c.execute(self.__clear)
            c.execute(self.__vacuum)
            c.commit() # unlock the database

    def wait(self, timeout, log):
        if len(self) > 0:
            ids = [str(j) for j in self.jobs()]
            log.info("See {} lsf jobs to wait for:\n\t{}".format(len(ids), "\n\t".join(ids)))
            bsub.poll(ids, timeout=timeout, log=log)
            self.clear()
            time.sleep(30) # wait a few seconds for the file system to catch up
        else:
            print("There are no LSF jobs to wait for!")
