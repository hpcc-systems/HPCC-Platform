/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifndef __THBUFDEF__
#define __THBUFDEF__


#define DEFAULT_LARGEMEM_BUFFER_SIZE                    (0x58000000)            // ~ 1.4GB  
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
#define JOINR_SMART_BUFFER_SIZE                 (0x100000*12)           // 12MB
#define LOOKUPJOINL_SMART_BUFFER_SIZE           (0x100000*12)           // 12MB
#define CATCH_BUFFER_SIZE                       (0x100000*12)           // 12MB
#define SKIPLIMIT_BUFFER_SIZE                   (0x100000*12)           // 12MB
#define WORKUNITWRITE_SMART_BUFFER_SIZE         (0x100000*3)            // 3MB
#define DEFAULT_BLOCK_INPUT_BUFFER_SIZE         (0x10000)               // 64K
#define AGGREGATE_INPUT_BUFFER_SIZE             (0x10000)               // 64K
#define NSPLITTER_SPILL_BUFFER_SIZE             (0x100000)              // 1MB
#define DISTRIBUTE_PULL_BUFFER_SIZE             (0x100000*32)           // 32MB
#define SORT_BUFFER_TOTAL                       (0x100000*20)           // 20MB (estimate)
#define DISTRIBUTE_SINGLE_BUFFER_SIZE           (0x10000)               // 64K  - NB per node and multiplied by async send
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



#endif
