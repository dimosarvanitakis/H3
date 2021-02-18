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

from . import h3lib

class H3List(list):
    """A list that has a ``done`` attribute. If ``done`` is ``False``
    there are more items to be fetched, so repeat the call
    with an appropriate offset to get the next batch.
    """
    def __new__(self, *args, **kwargs):
        return super().__new__(self, args, kwargs)

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and hasattr(args[0], '__iter__'):
            list.__init__(self, args[0])
        else:
            list.__init__(self, args)
        self.__dict__.update(kwargs)

    def __call__(self, **kwargs):
        self.__dict__.update(kwargs)
        return self

class H3Bytes(bytes):
    """A bytes object with a ``done`` attribute. If ``done`` is ``False``
    there is more data to be read, so repeat the call
    with an appropriate offset to get the next batch.
    """
    def __new__(self, *args, **kwargs):
        obj = super().__new__(self, *args)
        obj.__dict__.update(kwargs)
        return obj

class H3Version(type):
    @property
    def VERSION(self):
        return h3lib.version()

class H3Cache(object, metaclass=H3Version):
    """Python interface to H3.

    :param hot_storage_uri: cache backend storage URI
    :param cold_storage_uri: cold backend storage URI
    :param user_id: user performing all actions
    :type hot_storage_uri: string
    :type cold_storage_uri: string
    :type user_id: int

    Example backend URIs include (defaults for each type shown):

    * ``file:///tmp/h3`` for local filesystem
    * ``kreon://127.0.0.1:2181`` for Kreon, where the network location refers to the ZooKeeper host and port
    * ``rocksdb:///tmp/h3/rocksdb`` for `RocksDB <https://rocksdb.org>`_
    * ``redis://127.0.0.1:6379`` for `Redis <https://redis.io>`_

    .. note::
       All functions may raise standard exceptions on internal errors, or some ``pyh3lib.*Error``
       in respect to the underlying library's return values.
    """

    BUCKET_NAME_SIZE = h3lib.H3_BUCKET_NAME_SIZE
    """Maximum bucket name size."""

    OBJECT_NAME_SIZE = h3lib.H3_OBJECT_NAME_SIZE
    """Maximum object name size."""

    def __init__(self, cache_storage_uri, cold_storage_uri, user_id=0):
        self._cache_handle = h3lib.init(cache_storage_uri)
        self._cold_handle  = h3lib.init(cold_storage_uri)
        if not self._cache_handle:
            raise SystemError('Could not create H3 cache storage handle')
        if not self._cold_handle:
            raise SystemError('Could not create H3 cold storage handle')
        self._user_id = user_id

    def __write_data_to_cache__(self, bucket_name, object_name, data, offset=0):
        """Writes data to an object 
        
        The object MUST BE in the cache
        """

        # if the offset is 0 then write all the object from the start in the cache
        # no need to fetch it from the cold storage
        if not offset:
            # create the bucket in the cache
            try:
                h3lib.create_bucket(self._cache_handle, bucket_name) 
            except (h3lib.FailureError, h3lib.StoreError) as h3e:
                raise h3e
            # pass only the case that the bucket already exists
            except h3lib.ExistsError:
                pass
        else:
            # check if the object exists in the cache
            try:
                info = h3lib.info_object(self._cache_handle, bucket_name, object_name)
            except (h3lib.FailureError, h3lib.StoreError) as h3e:
                raise h3e
            # the object does not exist in cache
            except h3lib.NotExistsError:
                # fetch the object from the cold storage
                self.__fetch_data_from_cold__(bucket_name, object_name)

        try:
            return h3lib.write_object(self._cache_handle, bucket_name, object_name, data, offset, self._user_id)
        except (h3lib.FailureError, h3lib.StoreError) as h3e:
            raise h3e

    def __fetch_data_from_cold__(self, bucket_name, object_name, offset=0, size=0):
        """Fetches an object from the cold storage to the cache

        :returns: the fetched data
        """
        
        done        = False
        cold_offset = offset
        data        = b''

        # read all the object from the cold storage
        while not done:
            cold_data, done = h3lib.read_object(self._cold_handle, bucket_name, object_name, cold_offset, size, self._user_id)
            if cold_data is None:
                cold_data = b''
            data        += cold_data
            cold_offset += len(cold_data)

        # create the bucket if it is not exists
        try:
            h3lib.create_bucket(self._cache_handle, bucket_name) 
        # pass only the case that the bucket already exists
        except h3lib.ExistsError:
            pass
        
        # write it to the cache
        h3lib.write_object(self._cache_handle, bucket_name, object_name, data, 0, self._user_id)

        return data

    def list_buckets(self):
        """List all buckets in the cache & cold storage.

        :returns: A list of bucket names if the call was successful
        """

        return h3lib.list_buckets(self._cold_handle, self._user_id)

    def info_bucket(self, bucket_name, get_stats=False):
        """Get bucket information.

        :param bucket_name: the bucket name
        :param get_stats: include statistics in the reply (default is no)
        :type bucket_name: string
        :type get_stats: boolean
        :returns: A named tuple with bucket information if the call was successful

        The returned tuple has the following fields:

        ============  ===============
        ``creation``  <timestamp>
        ``stats``     <tuple> or None
        ============  ===============

        If stats is populated, it has the following fields:

        =====================  ===========
        ``size``               <int>
        ``count``              <int>
        ``last_access``        <timestamp>
        ``last_modification``  <timestamp>
        =====================  ===========

        .. note::
           A request to get stats requires iterating over all objects in the bucket, which may take significant time to complete.
        """
        
        return h3lib.info_bucket(self._cold_handle, bucket_name, get_stats, self._user_id)

    def create_bucket(self, bucket_name):
        """Create a bucket in the hot storage.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_bucket(self._cold_handle, bucket_name, self._user_id)

    def delete_bucket(self, bucket_name):
        """Delete a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful

        .. note::
           A bucket must be empty in order to be deleted.
        """

        return h3lib.delete_bucket(self._cold_handle, bucket_name, self._user_id)

    def purge_bucket(self, bucket_name):
        """Purge a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.purge_bucket(self._cold_handle, bucket_name, self._user_id)


    def list_objects(self, bucket_name, prefix='', offset=0, count=10000):
        """List objects in a bucket.

        :param bucket_name: the bucket name
        :param prefix: list only objects starting with prefix (default is no prefix)
        :param offset: continue list from offset (default is to start from the beginning)
        :param count: number of object names to retrieve
        :type bucket_name: string
        :type prefix: string
        :type offset: int
        :type count: int
        :returns: An H3List of object names if the call was successful
        """

        objects, done = h3lib.list_objects(self._cold_handle, bucket_name, prefix, offset, count, self._user_id)
        return H3List(objects, done=done)

    def info_object(self, bucket_name, object_name):
        """Get object information.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: A named tuple with object information if the call was successful

        The returned tuple has the following fields:

        =====================  ===========
        ``is_bad``             <boolean>
        ``size``               <int>
        ``creation``           <timestamp>
        ``last_access``        <timestamp>
        ``last_modification``  <timestamp>
        ``last_change``        <timestamp>
        =====================  ===========
        """

        return h3lib.info_object(self._cold_handle, bucket_name, object_name, self._user_id)

    def touch_object(self, bucket_name, object_name, last_access=-1, last_modification=-1):
        """Set object access and modification times (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param last_access: last access timestamp
        :param last_modification: last modification timestamp
        :type bucket_name: string
        :type object_name: string
        :type last_access: float
        :type last_modification: float
        :returns: ``True`` if the call was successful
        """

        return h3lib.touch_object(self._cold_handle, bucket_name, object_name, last_access, last_modification, self._user_id)

    def set_object_permissions(self, bucket_name, object_name, mode):
        """Set object permissions attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param mode: permissions mode
        :type bucket_name: string
        :type object_name: string
        :type mode: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.set_object_permissions(self._cold_handle, bucket_name, object_name, mode, self._user_id)
    
    def make_object_read_only(self, bucket_name, object_name):
        """Set object permissions attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.make_object_read_only(self._cold_handle, bucket_name, object_name, self._user_id)

    def set_object_owner(self, bucket_name, object_name, uid, gid):
        """Set object owner attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param uid: user id
        :param gid: group id
        :type bucket_name: string
        :type object_name: string
        :type uid: int
        :type gid: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.set_object_owner(self._cold_handle, bucket_name, object_name, uid, gid, self._user_id)

    def create_object(self, bucket_name, object_name, data):
        """Create an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the contents
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :returns: ``True`` if the call was successful
        """
        
        return h3lib.create_object(self._cold_handle, bucket_name, object_name, data, self._user_id) 

    def create_object_copy(self, bucket_name, src_object_name, offset, size, dst_object_name):
        """Create an object with data from another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type offset: int
        :type size: int
        :type dst_object_name: string
        :returns: The bytes copied if the call was successful
        """

        return h3lib.create_object_copy(self._cold_handle, bucket_name, src_object_name, offset, size, dst_object_name, self._user_id)
        
    def create_object_from_file(self, bucket_name, object_name, filename):
        """Create an object with data from a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_object_from_file(self._cold_handle, bucket_name, object_name, data, self._user_id)

    def write_object(self, bucket_name, object_name, data, offset=0):
        """Write to an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the data to write
        :param offset: the offset in the object where writing should start
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :type offset: int
        :returns: ``True`` if the call was successful
        """ 

        # first try with the cache
        try:
            print("write to cache")
            return self.__write_data_to_cache__(bucket_name, object_name, data, offset)
        # something went wrong, write it in the cold storage
        except (h3lib.FailureError, h3lib.StoreError) as h3e:
            print("write to cold, error")
            return h3lib.write_object(self._cold_handle, bucket_name, object_name, data, offset, self._user_id)

    def write_object_copy(self, bucket_name, src_object_name, src_offset, size, dst_object_name, dst_offset):
        """Write to an object with data from another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param src_offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param dst_object_name: the destination object name
        :param dst_offset: the offset in the destination object where writing should start
        :type bucket_name: string
        :type src_object_name: string
        :type src_offset: int
        :type size: int
        :type dst_object_name: string
        :type dst_offset: int
        :returns: The bytes copied if the call was successful
        """

        return h3lib.write_object_copy(self._cold_handle, bucket_name, src_object_name, src_offset, size, dst_object_name, dst_offset, self._user_id)

    def write_object_from_file(self, bucket_name, object_name, filename, offset=0):
        """Write to an object with data from a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :param offset: the offset in the object where writing should start
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :type offset: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.write_object_from_file(self._cache_handle, bucket_name, object_name, filename, offset, self._user_id)

    def read_object(self, bucket_name, object_name, offset=0, size=0):
        """Read from an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param offset: the offset in the object where reading should start
        :param size: the size of the data to read (default is all)
        :type bucket_name: string
        :type object_name: string
        :type offset: int
        :type size: int
        :returns: An H3Bytes object if the call was successful
        """

        done = False
        data = b''

        # try to fetch it from the cache
        try:
            print("read from the cache")
            data, done = h3lib.read_object(self._cache_handle, bucket_name, object_name, offset, size, self._user_id)
        
        # fetch it from the cold to the cache 
        except h3lib.NotExistsError:
            print("fetch to the cache")
            # bring the data form the cold to the cache 
            data = self.__fetch_data_from_cold__(bucket_name, object_name, offset)
            done = True
        
        # something went wrong with the cache
        except (h3lib.FailureError, h3lib.StoreError):
            data, done = h3lib.read_object(self._cold_handle, bucket_name, object_name, offset, size, self._user_id)

        return H3Bytes(data, done=done)

    def read_object_to_file(self, bucket_name, object_name, filename, offset=0, size=0):
        """Read from an object into a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :param offset: the offset in the object where reading should start
        :param size: the size of the data to read (default is all)
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :type offset: int
        :type size: int
        :returns: An H3Bytes object if the call was successful (empty data)
        """

        done = False

        # try to fetch it from the cache
        try:
            _, done = h3lib.read_object_to_file(self._cache_handle, bucket_name, object_name, filename, offset, size, self._user_id)
        
        # fetch it from the cold to the cache 
        except h3lib.NotExistsError:            
            # bring the data form the cold to the cache 
            self.__fetch_data_from_cold__(bucket_name, object_name, offset)

            # then read from the object from the cache
            _, done = h3lib.read_object_to_file(self._cache_handle, bucket_name, object_name, filename, offset, size, self._user_id)
        
        # something went wrong with the cache
        except (h3lib.FailureError, h3lib.StoreError):
            # read it from the cold storage
            _, done = h3lib.read_object_to_file(self._cold_handle, bucket_name, object_name, filename, offset, size, self._user_id)

        return H3Bytes(done=done)

    def copy_object(self, bucket_name, src_object_name, dst_object_name, no_overwrite=False):
        """Copy an object to another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :param no_overwrite: do not overwrite the destination if it exists (default is to overwrite)
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :type no_overwrite: boolean
        :returns: ``True`` if the call was successful
        """

        return h3lib.copy_object(self._cold_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

    def move_object(self, bucket_name, src_object_name, dst_object_name, no_overwrite=False):
        """Move/rename an object to another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :param no_overwrite: do not overwrite the destination if it exists (default is to overwrite)
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :type no_overwrite: boolean
        :returns: ``True`` if the call was successful
        """

        return h3lib.move_object(self._cold_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

    def exchange_object(self, bucket_name, src_object_name, dst_object_name):
        """Exchange data between objects.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.exchange_object(self._cold_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

    def truncate_object(self, bucket_name, object_name, size=0):
        """Read from an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param size: the new size of the object (default is 0)
        :type bucket_name: string
        :type object_name: string
        :type size: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.truncate_object(self._cold_handle, bucket_name, object_name, size, self._user_id)

    def delete_object(self, bucket_name, object_name):
        """Delete an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.delete_object(self._cold_handle, bucket_name, object_name, self._user_id)
    
    def create_object_metadata(self, bucket_name, object_name, metadata_name, metadata_value, size):
        """Create a new metadata for an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :param metadata_value: the object's metadata value
        :param size: the size of the object's metadata value
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :type metadata_value: bytes
        :type size: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, metadata_value, size, self._user_id)

    def delete_object_metadata(self, bucket_name, object_name, metadata_name):
        """Delete an object's specific metadata.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.delete_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, self._user_id)
    
    def read_object_metadata(self, bucket_name, object_name, metadata_name):
        """Delete an object's specific metadata.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :returns: An H3Bytes object if the call was successful
        """

        data, done = h3lib.read_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, self._user_id)
        if data is None:
            data = b''
        return H3Bytes(data, done=done)
    
    def list_objects_with_metadata(self, bucket_name, metadata_name):
        """Delete an object's specific metadata.

        :param bucket_name: the bucket name
        :param metadata_name: metadata name
        :type bucket_name: string
        :type metadata_name: string
        :returns: An H3List of object names if the call was successful
        """

        objects, done = h3lib.list_objects_with_metadata(self._cold_handle, bucket_name, metadata_name, self._user_id)
        return H3List(objects, done=done)

    def list_multiparts(self, bucket_name, offset=0, count=10000):
        """List all multipart IDs for a bucket.

        :param bucket_name: the bucket name
        :param offset: continue list from offset (default is to start from the beginning)
        :param count: number of multipart ids to retrieve
        :type bucket_name: string
        :type offset: int
        :type count: int
        :returns: An H3List of multipart ids if the call was successful
        """

        multiparts, done = h3lib.list_multiparts(self._cold_handle, bucket_name, offset, count, self._user_id)
        return H3List(multiparts, done=done)

    def create_multipart(self, bucket_name, object_name):
        """Create a multipart object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: The multipart id if the call was successful
        """

        return h3lib.create_multipart(self._cold_handle, bucket_name, object_name, self._user_id)

    def complete_multipart(self, multipart_id):
        """Complete a multipart object (creates the actual object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.complete_multipart(self._cold_handle, multipart_id, self._user_id)

    def abort_multipart(self, multipart_id):
        """Abort a multipart object (deletes the multipart object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.abort_multipart(self._cold_handle, multipart_id, self._user_id)

    def list_parts(self, multipart_id):
        """List all parts of a multipart object.

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: A list of named tuples, each with part information if the call was successful

        The returned list contains tuples having the following fields:

        ===============  =====
        ``part_number``  <int>
        ``size``         <int>
        ===============  =====
        """

        return h3lib.list_parts(self._cold_handle, multipart_id, self._user_id)

    def create_part(self, multipart_id, part_number, data):
        """Create a part of a multipart object.

        :param multipart_id: the multipart id
        :param part_number: the part to create
        :param data: the contents
        :type multipart_id: string
        :type part_number: int
        :type data: bytes
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_part(self._cold_handle, multipart_id, part_number, data, self._user_id)

    def create_part_copy(self, object_name, offset, size, multipart_id, part_number):
        """Create a part of a multipart object with data from another object.

        :param object_name: the source object name
        :param offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param multipart_id: the multipart id
        :param part_number: the part to create
        :type object_name: string
        :type offset: int
        :type size: int
        :type multipart_id: string
        :type part_number: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_part_copy(self._cold_handle, object_name, offset, size, multipart_id, part_number, self._user_id)
