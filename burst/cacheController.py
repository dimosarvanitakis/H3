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
import time

DEFAULT_EXPIRES_AT_TIME = 1800 
"""Maximum default time that the objects stay in the cache (30 minutes)"""

def write_back_to_cold(h3_cache, h3_cold, h3_bucket, h3_object, h3_metadata):
    done   = False
    offset = 0
    
    while not done:
        data = h3_cache.read_object(h3_bucket, h3_object, offset)
        h3_cold.write_object(h3_bucket, h3_object, data, offset)
        
        done    = data.done
        offset += len(data)

    # delete the CachedAt attribute
    h3_cold.delete_object_metadata(h3_bucket, h3_object, h3_metadata)
    
    # remove the object from the cache
    h3_cache.delete_object(h3_bucket, h3_object)

def move_to_cold(h3_cache, h3_cold, expires_time):
    """
    Moves the objects from the the cache storage to 
    the cold storage

    :param h3_cache: h3 cache storage
    :param h3_cold: h3 cold storage
    :param now : the "now" time
    :type h3_cache: object
    :type h3_cold: object
    :type now: float
    :returns: nothing
    """

    now = time.get_time(time.CLOCK_REALTIME)

    # List all the buckets in the cache storage
    for h3_bucket in h3_cache.list_buckets():
        # List all the objects in the cache bucket
        for h3_object in h3_cache.list_objects(h3_bucket):
            # get when it expires from the cache    
            cached_at = struct.unpack('d', h3_cold.read_object_metadata(h3_bucket, h3_object, 'CachedAt'))
            # the user has defined a specific time from the object to be delete it from the cache
            expires_from_cache = struct.unpack('d', h3_cold.read_object_metadata(h3_bucket, h3_object, 'ExpireFromCache'))

            # the object must be moved back to the cold storage
            if expires_from_cache and (expires_from_cache <= now):
                write_back_to_cold(h3_cache, h3_cold, h3_bucket, h3_object, 'ExpireFromCache')

                # delete the CachedAt attribute
                h3_cold.delete_object_metadata(h3_bucket, h3_object, 'CachedAt')

            elif cached_at and (cached_at[0] + expires_time) >= now:
                write_back_to_cold(h3_cache, h3_cold, h3_bucket, h3_object, 'CachedAt')

def main(cmd=None):
    parser = argparse.ArgumentParser(description='Move Objects to Cold storage')
    parser.add_argument('--cache_storage', required=True,  help=f'Cache H3 storage URI.')
    parser.add_argument('--cold_storage',  required=True,  help=f'Cold H3 storage URI.')
    parser.add_argument('--expires_time',  required=False, help=f'The maximum time that an object allowed to stay in the cache (in seconds). The default value is 30 Minutes.', default=DEFAULT_EXPIRES_AT_TIME)
    
    args = parser.parse_args(cmd)
    cache_url = args.cache_storage 
    cold_url  = args.cold_storage
    expires_time = args.expires_time

    if cache_url and cold_url:
        h3_cache = pyh3lib.H3(cache_url)
        h3_cold  = pyh3lib.H3(cold_url)

        # Move the objects from the cache storage to the cold storage   
        move_to_cold(h3_cache, h3_cold, expires_time)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()