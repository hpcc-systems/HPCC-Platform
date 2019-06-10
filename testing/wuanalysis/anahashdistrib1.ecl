/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

// Analysis should show: DISTRIBUTE output skew is worse than input skew
//
// NOTE: For faster nodes, it may be necessary to increase the size of testfile

IMPORT * From common;

visits := DATASET(testfile1, layout_visits, THOR);

layout_visitCounts := RECORD
  visits.url;
  visits_cnt := COUNT(GROUP);
END;

visitcounts := TABLE(DISTRIBUTE(visits,HASH32(url[1..3])),
                     layout_visitCounts,url,LOCAL);
OUTPUT(visitcounts);

