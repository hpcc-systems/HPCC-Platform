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

activityRecord := RECORD
    UNSIGNED id{XPATH('@id')};
    STRING label{XPATH('@label')};
    UNSIGNED kind{XPATH('att[name="kind"]/@value')};
END;

subGraphRecord := RECORD
    UNSIGNED id{XPATH('@id')};
    DATASET(activityRecord) activities{XPATH('att/graph/node')};
END;

graphRecord := RECORD
    STRING name{XPATH('@name')};
    DATASET(subGraphRecord) subgraphs{XPATH('xgmml/graph/node')};
END;


ds := PIPE('cmd /C type c:\\temp\\a.xml', graphRecord, xml('*/Graphs/Graph'));
output(ds);

ds1 := DATASET('c:\\temp\\a.xml', graphRecord, xml('*/Graphs/Graph'));
output(ds1);
