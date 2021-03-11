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
import random

MEGABYTE = 1048576
COUNT    = 100
WRITES   = 5

def test_setup(h3):
    with open('/dev/urandom', 'rb') as f:
        data = f.read(10 * MEGABYTE)

    h3.create_bucket("b1")

    for objects in range(COUNT):
        h3.create_object("b1", "o%d" % objects, data) 

def test_simple_without_offset(h3):
    with open('/dev/urandom', 'rb') as f:
        data = f.read(100 * MEGABYTE)

    for objects in range(COUNT):
        h3.write_object("b1", "o%d" % objects, data)

    samples = random.sample(range(COUNT), 10)
    
    for writes in range(WRITES):
        for sample in samples:
            h3.write_object("b1", "o%d" % objects, data)

def test_simple_with_offset(h3):
    with open('/dev/urandom', 'rb') as f:
        data = f.read(100 * MEGABYTE)

    for objects in range(COUNT):
        h3.write_object("b1", "o%d" % objects, data)

    samples = random.sample(range(COUNT), 10)
    
    for writes in range(WRITES):
        for sample in samples:
            h3.write_object("b1", "o%d" % objects, data, MEGABYTE)

def main(cmd=None):
    parser = argparse.ArgumentParser(description='Move Objects to Cold storage')
    parser.add_argument('--storage', required=True, help=f'Cold H3 storage URI')
    
    args = parser.parse_args(cmd)
    storage = args.storage

    if storage:
        h3_simple  = pyh3lib.H3(storage)

        test_setup(h3_simple)
        
        start_time = time.time()
        test_simple_without_offset(h3_simple)
        print("--- %s Simple Without Offset Seconds --- \n" % (time.time() - start_time))
        
        start_time = time.time()
        test_simple_with_offset(h3_simple)
        print("--- %s Simple With Offset Seconds --- \n" % (time.time() - start_time))
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()