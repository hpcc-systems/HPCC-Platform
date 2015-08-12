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



namesRecord :=
            RECORD
unsigned6       did;
string20        surname;
string10        forename;
integer2        age := 25;
string100       extra;
            END;

slimRecord :=
            RECORD
unsigned6       did;
string20        surname;
string10        forename;
            END;


processNameLibrary(dataset(namesRecord) ds) := interface
    export dataset(slimRecord) slim;
    export dataset(namesRecord) matches;
    export dataset(namesRecord) mismatches;
end;


oldNamesTable := dataset('oldNames',namesRecord,FLAT);
newNamesTable := dataset('newNames',namesRecord,FLAT);

processedOld := LIBRARY('ProcessNameLibrary', processNameLibrary(oldNamesTable));
processedNew := LIBRARY('ProcessNameLibrary', processNameLibrary(newNamesTable));


allSlim := processedOld.slim + processedNew.slim;           // record is based on input parameters, so still not full bound before normalization....

output(allSlim);

