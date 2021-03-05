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

def MoveToCold(h3_cache, h3_cold):
    """
    Moves the objects from the the cache storage to 
    the cold storage

    :param h3_cache: h3 cache storage
    :param h3_cold: h3 cold storage
    :type h3_cache: object
    :type h3_cold: object
    :returns: nothing
    """

    # List all the buckets in the cache storage
    for h3_cache_bucket in h3_cache.list_buckets():
        # List all the objects in the cache bucket
        for h3_cache_object in h3_cache.list_objects(h3_cache_bucket):
            done   = False
            offset = 0
            
            while not done:
                data = h3_cache.read_object(h3_cache_bucket, h3_cache_object, offset)
                h3_cold.write_object(h3_cache_bucket, h3_cache_object, data, offset)
                
                done    = data.done
                offset += len(data)

            h3_cache.delete_object(h3_cache_bucket, h3_cache_object)

def main(cmd=None):
    parser = argparse.ArgumentParser(description='Move Objects to Cold storage')
    parser.add_argument('--cache_storage', required=True, help=f'Cache H3 storage URI')
    parser.add_argument('--cold_storage', required=True, help=f'Cold H3 storage URI')
    
    args = parser.parse_args(cmd)
    cache_config_path = args.cache_storage 
    cold_config_path = args.cold_storage

    print(cache_config_path, cold_config_path)

    if cache_config_path and cold_config_path:
        h3_cache = pyh3lib.H3(cache_config_path)
        h3_cold  = pyh3lib.H3(cold_config_path)

        # Move the objects from the hot storage to the cold storage   
        MoveToCold(h3_cache, h3_cold)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()