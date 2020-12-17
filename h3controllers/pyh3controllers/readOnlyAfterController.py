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

def makeObjectsReadOnly(now):
    """
    Set's the permissions to read only in all objects 
    that have the metadata key ReadOnlyAfter, and the 
    time that is specified in the ReadOnlyAfter plus
    the time from the last time that the object has 
    been modified exceeds the now time. 
    
    :param now: the time, now
    :type now: float
    :returns: nothing
    """

    # list all the buckets 
    for h3_bucket in H3.list_buckets():
        # list all the objects with the specific metadata key
        for h3_object in H3.list_objects_with_metadata(h3_bucket, "ReadOnlyAfter"):
            # the h3_object contains the object's name
            h3_object_read_only_secs = float(H3.read_object_metadata(h3_bucket, h3_object, "ReadOnlyAfter"))
            h3_object_info = H3.info_object(h3_bucket, h3_object)

            # Check if we must change the permissions of the object to read only
            if (h3_object_info.last_modification + h3_object_read_only_secs >= now):
                H3.make_object_read_only(h3_bucket, h3_object)

def main():
    # Wall Clock
    clock = time.CLOCK_REALTIME
    # Pass the time     
    makeObjectsReadOnly(time.clock_gettime(clock))

if __name__ == '__main__':
    main()
         