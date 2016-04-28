/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef __THBUFDEF__
#define __THBUFDEF__


#define SMALL_SMART_BUFFER_SIZE                 (0x100000)              // 1MB
#define PULL_SMART_BUFFER_SIZE                  (0x100000*8*3)          // 24MB
#define CHOOSESETS_SMART_BUFFER_SIZE            (0x100000*8*3)          // 12MB
#define CHOOSESETSPLUS_SMART_BUFFER_SIZE        (0x100000*8*3)          // 12MB
#define FIRSTN_SMART_BUFFER_SIZE                (0x100000*3)            // 3MB
#define SELECTN_SMART_BUFFER_SIZE               (0x100000*3)            // 3MB
#define ITERATE_SMART_BUFFER_SIZE               (0x100000*12)           // 12MB
#define ROLLUP_SMART_BUFFER_SIZE                (0x100000*12)           // 12MB
#define PROCESS_SMART_BUFFER_SIZE               (0x100000*12)           // 12MB
#define DEDUP_SMART_BUFFER_SIZE                 (0x100000*12)           // 12MB
#define INDEXWRITE_SMART_BUFFER_SIZE            (0x100000*12)           // 12MB
#define COUNTPROJECT_SMART_BUFFER_SIZE          (0x100000*12)           // 12MB
#define ENTH_SMART_BUFFER_SIZE                  (0x100000*12)           // 12MB
#define JOIN_SMART_BUFFER_SIZE                  (0x100000*12)           // 12MB
#define LOOKUPJOINL_SMART_BUFFER_SIZE           (0x100000*12)           // 12MB
#define CATCH_BUFFER_SIZE                       (0x100000*12)           // 12MB
#define SKIPLIMIT_BUFFER_SIZE                   (0x100000*12)           // 12MB
#define WORKUNITWRITE_SMART_BUFFER_SIZE         (0x100000*3)            // 3MB
#define DEFAULT_BLOCK_INPUT_BUFFER_SIZE         (0x10000)               // 64K
#define AGGREGATE_INPUT_BUFFER_SIZE             (0x10000)               // 64K
#define NSPLITTER_SPILL_BUFFER_SIZE             (0x100000)              // 1MB
#define DISTRIBUTE_PULL_BUFFER_SIZE             (0x100000*32)           // 32MB
#define SORT_BUFFER_TOTAL                       (0x100000*20)           // 20MB (estimate)
#define DISTRIBUTE_DEFAULT_OUT_BUFFER_SIZE      (0x100000)              // 1MB (* targets (numnodes), on each slave)
#define DISTRIBUTE_DEFAULT_IN_BUFFER_SIZE       (0x100000*32)           // 32MB input buffer (on each slave)
#define FUNNEL_MIN_BUFF_SIZE                    (0x100000*2)            // 2MB
#define FUNNEL_MAX_BUFF_SIZE                    (0x100000*20)           // 20MB
#define COMBINE_MAX_BUFF_SIZE                   (0x100000*20)           // 20MB
#define FUNNEL_PERINPUT_BUFF_SIZE               (0x10000)               // 64K
#define PIPETHROUGH_BUFF_SIZE                   (0x10000)               // 64K
#define DEFAULT_ROWSERVER_BUFF_SIZE             (0x10000)               // 64K
#define MERGE_TRANSFER_BUFFER_SIZE              (0x10000)               // 64K
#define EXCESSIVE_PARALLEL_THRESHHOLD           (0x500000)              // 5MB
#define LOOP_SMART_BUFFER_SIZE                  (0x100000*12)           // 12MB
#define LOCALRESULT_BUFFER_SIZE                 (0x100000*10)           // 10MB

#define DEFAULT_KEYNODECACHEMB                  10
#define DEFAULT_KEYLEAFCACHEMB                  50
#define DEFAULT_KEYBLOBCACHEMB                  0

#define DISTRIBUTE_RESMEM(N) ((DISTRIBUTE_DEFAULT_OUT_BUFFER_SIZE * (N)) + DISTRIBUTE_DEFAULT_IN_BUFFER_SIZE)


#endif
