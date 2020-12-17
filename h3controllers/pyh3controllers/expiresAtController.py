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

from pyh3lib import H3

def removeObjectsAfter(now):
    """
    Delete's all the objects that have the metadata 
    key ExpiresAt, and the time that is specified in
    the ExpiresAt has come.

    :param now: the time, now
    :type now: float
    :returns: nothing
    """

    # list all the buckets 
    for h3_bucket in H3.list_buckets():
        # list all the objects with the specific metadata key
        for h3_object in H3.list_objects_with_metadata(h3_bucket, "ExpiresAt"):
            # the h3_object contains the object's name
            h3_object_remove_timestamp = float(H3.read_object_metadata(h3_bucket, h3_object, "ExpiresAt"))

            # Check if we must change the permissions of the object to read only
            if (h3_object_remove_timestamp <= now) :
                H3.delete_object(h3_bucket, h3_object)

def main():
    # Wall Clock
    clock = time.CLOCK_REALTIME
    # Pass the time     
    removeObjectsAfter(time.clock_gettime(clock))

if __name__ == '__main__':
    main()
