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

import time 
import argparse
import struct

import pyh3lib

def ReadOnlyAfter(h3):
    """
    Set's the permissions to read only in all objects that have the ReadOnlyAfter attribute and the "now" time exceeds
    the time that is specified in the ReadOnlyAfter plus the time from the last time that the object has been modified.
    
    :param now: the time, now
    :type now: float
    :returns: nothing
    """

    now = time.clock_gettime(time.CLOCK_REALTIME)

    # list all the buckets 
    for h3_bucket in h3.list_buckets():
        done   = False
        offset = 0
        
        # list all the objects that have the ExpiresAt attribute
        while not done:
            h3_objects = h3.list_objects_with_metadata(h3_bucket, "ReadOnlyAfter", offset)

            for h3_object in h3_objects:
                # the h3_object contains the object's name
                read_only_after = h3.read_object_metadata(h3_bucket, h3_object, "ReadOnlyAfter")

                if read_only_after != b'':
                    # try to parse
                    try:
                        h3_object_remove_timestamp = struct.unpack('d', read_only_after)
                    except struct.error:
                        continue

                    h3_object_info = h3.info_object(h3_bucket, h3_object)

                    # Check if we must change the permissions of the object to read only
                    if ((not h3_object_info.read_only) and (now >= (h3_object_info.last_modification + h3_object_remove_timestamp[0]))):
                        h3.make_object_read_only(h3_bucket, h3_object)

            done    = h3_objects.done
            offset  = h3_objects.nextOffset

def main(cmd=None):
    parser = argparse.ArgumentParser(description='ReadOnlyAfter Controller')
    parser.add_argument('--storage', required=True, help=f'H3 storage URI')
    
    args = parser.parse_args(cmd)
    config_path = args.storage 
    
    if config_path:
        h3 = pyh3lib.H3(config_path)

        ReadOnlyAfter(h3)
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
         