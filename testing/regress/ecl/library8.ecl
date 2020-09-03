/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

//nothor

//Catch corruption when the wrong meta is returned for a library call result
#option ('checkingHeap', true);

smallRecord := { unsigned id; };
largeRecord := { unsigned id; string1000 bigid; };


FilterDatasetInterface(dataset(smallRecord) ds, unsigned every) := interface
    export dataset(smallRecord) result1;
    export dataset(largeRecord) result2;
end;


FilterDatasetLibrary(dataset(smallRecord) ds, unsigned every) := module,library(FilterDatasetInterface)
    export result1 := ds;
    export result2 := project(ds((id % every) = 1), transform(largeRecord, SELF.id := LEFT.id; SELF.bigid := (string)LEFT.id));
end;



filterDataset(dataset(smallRecord) ds, unsigned every) := library(internal(FilterDatasetLibrary), FilterDatasetInterface(ds, every));


recs := DATASET(100, transform(smallRecord, SELF.id := COUNTER));

filtered := filterDataset(recs, 2);
j := JOIN(recs(id > 40), filtered.result2, LEFT.id = RIGHT.id, left outer);
output(count(nofold(j)));
