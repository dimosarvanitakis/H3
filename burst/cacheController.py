# Copyright [2019] [FORTH-ICS]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import pyh3lib
import threading
import signal
from datetime import timedelta

DEFAULT_EXPIRES_AT_TIME = 1800 
"""Maximum default time that the objects stay in the hot storage (30 minutes)"""

EXPIRE_INTERVAL = 1200 
"""Periodical time that an expiration event will executed (20 minutes default)"""

EVICT_INTERVAL = 600 
"""Periodical time that an eviction event will executed (10 minutes default)"""

storageLock = threading.Lock()
"""A lock to make sure that the eviction event and the expire event is not running at the same time"""

class CacheControllerJob(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.interval = interval
        self.execute  = execute
        
        self.daemon   = False
        self.stopped  = threading.Event()
        self.args     = args
        self.kwargs   = kwargs
        
    def stop(self):
                self.stopped.set()
                self.join()

    def run(self):
            while not self.stopped.wait(self.interval.total_seconds()):
                self.execute(*self.args, **self.kwargs)

def WatermarkRange(arg):
    """A range check function for the watermark arguments.

    :param arg: The watermark argument. 
    :type arg: unknown 
    """   

    try:
        percentage = int(arg)
    except ValueError:    
        raise argparse.ArgumentTypeError("The percentage must be an integer number")
    if percentage < 0 or percentage > 100:
        raise argparse.ArgumentTypeError("The percentage argument must be > 0 and < 100")
    return percentage

def WriteBackToCold(h3_hot, h3_cold, h3_bucket, h3_object):
    """Moves an object from the hot storage back to the cold.

    :param h3_hot: The H3 hot storage. 
    :param h3_cold: The H3 cold storage.
    :param h3_bucket: The bucket that the object belongs to.
    :param h3_object: The object that we need to move.
    :type h3_hot: object
    :type h3_cold: object
    :type h3_bucket: string
    :type h3_object: string
    """   

    done   = False
    offset = 0
    
    while not done:
        data = h3_hot.read_object(h3_bucket, h3_object, offset)
        h3_cold.write_object(h3_bucket, h3_object, data, offset)
        
        done    = data.done
        offset += len(data)
    
    # remove the object from the hot storage
    h3_hot.delete_object(h3_bucket, h3_object)

def EvictObjects(h3_hot, h3_cold, watermark_low, watermark_high): 
    """Checks if the hot storage is nearly full (based on the watermark_high w)
       in order to start evicting objects, until we reach the watermark_low 
       percentage.

    :param h3_hot: The H3 hot storage. 
    :param h3_cold: The H3 cold storage.
    :param watermark_low: The percentage of the disk we need to reach if we start evicting.
    :param watermark_high: The percentage of the disk we need to reach to start evicting.
    :type h3_hot: object
    :type h3_cold: object
    :type watermark_low: int
    :type watermark_high: int

    .. note::
       The objects eviction follows the LRU policy.
    """

    # acquire the storage lock
    storageLock.acquire()

    # take the cache usage
    storage_info = h3_hot.info_storage()

    # if the hot storage has reached the high watermark start to evict objects
    if storage_info and ( storage_info.used_space >= int(watermark_high * storage_info.total_space / 100) ):
        
        lru = []

        # iterate all the objects in the cache
        for h3_bucket in h3_hot.list_buckets():
            for h3_object in h3_hot.list_objects(h3_bucket):
                
                # take the info for each object
                info = h3_cold.info_object(h3_bucket, h3_object)

                # Todo(dimos): we can add a size threshold
                # append it to the lru list
                lru.append({
                    'bucket'  : h3_bucket,
                    'object'  : h3_object,
                    'modified': info.last_modification,
                    'size'    : info.size
                })

        lru = sorted(lru, key = lambda obj: (obj['modified'], obj['size']))

        watermark_low_size  = int(watermark_low * storage_info.total_space / 100)
        watermark_high_size = storage_info.used_space

        # start the eviction
        for lru_object in lru:
            
            h3_bucket = lru_object['bucket']
            h3_object = lru_object['object']

            # evict the object from the cache
            WriteBackToCold(h3_hot, h3_cold, h3_bucket, h3_object)

            try:
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'ExpireFromCache')
            except pyh3lib.H3NotExistsError:
                pass
            
            try:
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'CachedAt')
            except pyh3lib.H3NotExistsError:
                pass

            # check if we reached the low watermark in order to stop the eviction
            watermark_high_size -= lru_object['size']
            if watermark_high_size <= watermark_low_size:
                break

    # release the storage lock
    storageLock.release()
            
def ExpireObjects(h3_hot, h3_cold, expires_time):
    """Checks all the objects in the hot storage if some of them 
       have expired in order to evict them.

    :param h3_hot: The H3 hot storage. 
    :param h3_cold: The H3 cold storage.
    :param expire_time: The time after an object assumed expired.
    :type h3_hot: object
    :type h3_cold: object
    :type expire_time: int

    .. note::
       The objects expiration is based on the ``CachedAt`` and 
       the `ExpireFromCache` attributes of an object.
    """   

    # acquire the storage lock
    storageLock.acquire()

    now = time.get_time(time.CLOCK_REALTIME)

    # List all the buckets in the hot storage
    for h3_bucket in h3_hot.list_buckets():
        # List all the objects in the hot bucket
        for h3_object in h3_hot.list_objects(h3_bucket):
            # get when it expires from the hot
            try:
                cached_at = h3_cold.read_object_metadata(h3_bucket, h3_object, 'CachedAt')
                cached_at = struct.unpack('d', cached_at)
            except pyh3lib.H3NotExistsError:
                continue
            
            # the user has defined a specific time from the object to be delete it from the hot
            try:
                expires_from_cache = h3_cold.read_object_metadata(h3_bucket, h3_object, 'ExpireFromCache')
                expires_from_cache = struct.unpack('d', expires_from_cache)
            except pyh3lib.H3NotExistsError:
                continue

            # the object must be moved back to the cold storage
            if expires_from_cache and (expires_from_cache <= now):
                WriteBackToCold(h3_hot, h3_cold, h3_bucket, h3_object)

                # delete the CachedAt attribute
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'CachedAt')

                # delete the CachedAt attribute
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'ExpireFromCache')

            elif cached_at and (cached_at[0] + expires_time) >= now:
                WriteBackToCold(h3_hot, h3_cold, h3_bucket, h3_object)

                # delete the CachedAt attribute
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'CachedAt')
    
    # release the storage lock
    storageLock.release()

def main(cmd=None):
    parser = argparse.ArgumentParser(description='Move Objects to Cold storage')
    parser.add_argument('--hot_storage',     required=True,  help=f'Hot H3 storage URI.')
    parser.add_argument('--cold_storage',    required=True,  help=f'Cold H3 storage URI.')
    parser.add_argument('--watermark_low',   required=False, help=f'The percentage of the disk to stop evicting.',  type=WatermarkRange, default=50)
    parser.add_argument('--watermark_high',  required=False, help=f'The percentage of the disk to start evicting.', type=WatermarkRange, default=90)
    parser.add_argument('--expires_time',    required=False, help=f'The maximum time that an object allowed to stay in the hot storage (in seconds). The default value is 30 minutes.', default=DEFAULT_EXPIRES_AT_TIME)
    parser.add_argument('--expire_interval', required=False, help=f'The interval between two expiration events (in seconds). The default value is 20 minutes.',                         default=EXPIRE_INTERVAL)
    parser.add_argument('--evict_interval',  required=False, help=f'The interval between two eviction events (in seconds). The default value is 10 minutes.',                           default=EVICT_INTERVAL)
    
    args = parser.parse_args(cmd)
    hot_url         = args.hot_storage 
    cold_url        = args.cold_storage
    watermark_low   = args.watermark_low
    watermark_high  = args.watermark_high
    expires_time    = args.expires_time
    expire_interval = args.expire_interval
    evict_interval  = args.evict_interval

    if hot_url and cold_url:
        h3_hot = pyh3lib.H3(hot_url)
        h3_cold  = pyh3lib.H3(cold_url)
        
        expire_job = CacheControllerJob(interval=timedelta(seconds=expire_interval), execute=ExpireObjects, args=(h3_hot, h3_cold, watermark_low, watermark_high,) )
        expire_job.start()

        evict_job = CacheControllerJob(interval=timedelta(seconds=evict_interval), execute=EvictObjects, args=(h3_hot, h3_cold, expires_time,) )
        evict_job.start()
        
        while True:
            try:
                continue
            except InterruptedError:
                print("Cache controller has been stopped. Running clean up code")
                evict_job.stop()
                expire_job.stop()
                break
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()