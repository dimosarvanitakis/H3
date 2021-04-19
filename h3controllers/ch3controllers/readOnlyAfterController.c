// Copyright [2019] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <time.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <h3lib/h3lib.h>

#define TIMESPEC_TO_DOUBLE(t)   (t.tv_sec + ((double)t.tv_nsec / 1000000000ULL))
#define TRUE                    1

void PrintUsage() {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "\t-s <string>  H3 storage URI\n");
    fprintf(stderr, "\t-h           This help messsage\n\n");
}

int main(int argc, char* argv[]) {    
    char* url;

    if (argc < 2) {
        PrintUsage();
        return 1;
    }

    int opt;
    while ((opt = getopt(argc, argv, ":s:h")) != -1) {
        switch (opt) {
            case 's':{
                url = strdup(optarg);
            }break;
            
            case 'h':{
                PrintUsage();
                
                return 0;
            }break;
        
            default:{
                PrintUsage();
                
                return 1;
            }
        }
    }  
    
    // Get the time now
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    // Authentication
    H3_Auth auth;
    auth.userId = 0;
    
    H3_Handle h3_handle = H3_Init(url);
    if (!h3_handle) {
        fprintf(stderr, "[ERROR] : fail to init H3.\n");
        return 1;
    }

    H3_Name buckets = NULL;
    uint32_t totalBuckets; 

    // List all the buckets    
    if (H3_ListBuckets(h3_handle, &auth, &buckets, &totalBuckets) == H3_SUCCESS) {
        H3_Name current_bucket;
        uint32_t current_bucket_pos = 0;
        size_t current_bucket_len = 0;
        int bucket;
        for (bucket = 0; bucket < totalBuckets; bucket++) {
            current_bucket = &(buckets[current_bucket_pos]);
            current_bucket_len = strlen(current_bucket);
            current_bucket_pos += current_bucket_len;
            while (buckets[current_bucket_pos] == '\0')
                current_bucket_pos++;
            
            H3_Name objects = NULL;
            H3_Status status;
            uint32_t totalObjects = 0;
            uint32_t offset = 0;
            uint32_t nextOffset = 0;

            // List all the objects that have an ReadOnlyAfter metadata
            while ((status = H3_ListObjectsWithMetadata(h3_handle, &auth, current_bucket, "ReadOnlyAfter", offset, &objects, &totalObjects, &nextOffset)) == H3_CONTINUE || (status == H3_SUCCESS && totalObjects)) {
                H3_Name current_object;
                uint32_t current_object_pos = 0;
                size_t current_object_len = 0;
                int object;
                for (object = 0; object < totalObjects; object++) {
                    current_object = &(objects[current_object_pos]);
                    current_object_len = strlen(current_object);
                    current_object_pos += current_object_len;
                    while (objects[current_object_pos] == '\0')
                        current_object_pos++;

                    void* readOnly;
                    size_t readOnlySize;
                    // Read the ExpiresAt value
                    if (H3_ReadObjectMetadata(h3_handle, &auth, current_bucket, current_object, "ReadOnlyAfter", &readOnly, &readOnlySize) == H3_SUCCESS) {
                        double readOnlyTime = 0.0f;
                        H3_ObjectInfo objectInfo;
                        
                        // Copy the readOnly;
                        memcpy(&readOnlyTime, readOnly, readOnlySize);

                        if (H3_InfoObject(h3_handle, &auth, current_bucket, current_object, &objectInfo) == H3_SUCCESS) {
                            // Check if the object must become readonly
                            fprintf(stdout, "Now : [%lf] ~ ReadOnlyTime : [%lf]\n", TIMESPEC_TO_DOUBLE(now), TIMESPEC_TO_DOUBLE(objectInfo.lastModification) + readOnlyTime);
                            if (TIMESPEC_TO_DOUBLE(objectInfo.lastModification) + readOnlyTime >= TIMESPEC_TO_DOUBLE(now)) {
                                H3_Attribute readOnly;
                                readOnly.type = H3_ATTRIBUTE_READ_ONLY;
                                readOnly.readOnly = TRUE;
                                
                                H3_SetObjectAttributes(h3_handle, &auth, current_bucket, current_object, readOnly);
                            }
                        } 
                    }

                    // Free 
                    if (readOnly)
                        free(readOnly);
                }

                offset = nextOffset;
            }

            // Free 
            if (objects)
                free(objects);
        }
    }
 
    // Free resources
    if (buckets) 
        free(buckets);
    if (url)    
        free(url);
    H3_Free(h3_handle);

    return 0;
}