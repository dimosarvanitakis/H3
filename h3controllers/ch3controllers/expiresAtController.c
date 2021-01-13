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

#define TIMESPEC_TO_DOUBLE(t) (t.tv_sec + ((double)t.tv_nsec / 1000000000ULL))

void PrintUsage() {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "\t-s <string>  H3 storage URI\n");
    fprintf(stderr, "\t-h           This help messsage\n\n");
}

int main(int argc, char* argv[]) {    
    char* url;

    int opt;

    if (argc < 2) {
        PrintUsage();
        return 1;
    }

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
        return -1;
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
            uint32_t totalObjects;

            // List all the objects that have an ExpiresAt metadata
            if (H3_ListObjectsWithMetadata(h3_handle, &auth, current_bucket, "ExpiresAt", &objects, &totalObjects) == H3_SUCCESS) {
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

                    void* expiresAt;
                    size_t expiresAtSize;
                    // Read the ExpiresAt value
                    if (H3_ReadObjectMetadata(h3_handle, &auth, current_bucket, current_object, "ExpiresAt", &expiresAt, &expiresAtSize) == H3_SUCCESS) {
                        double expiresAtTime = 0.0f;
                        
                        // Copy the expiresAt  
                        memcpy(&expiresAtTime, expiresAt, expiresAtSize);

                        fprintf(stdout, "Now : [%lf] ~ ExpiresAt : [%lf]\n", TIMESPEC_TO_DOUBLE(now), expiresAtTime);

                        // Check if the object must be deleted
                        if (TIMESPEC_TO_DOUBLE(now) >= expiresAtTime) {
                            H3_DeleteObject(h3_handle, &auth, current_bucket, current_object);
                        }
                    }

                    // Free 
                    if (expiresAt)
                        free(expiresAt);
                }
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