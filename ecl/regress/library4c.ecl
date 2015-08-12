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

//Example of nested
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;


filterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;


hallidayLibrary(dataset(namesRecord) ds) := module(LIBRARY('filterLibrary', filterLibrary(ds, 'Hawthorn', false)))
    export both := if (count(included) > 0, excluded, excluded(age != 0));
end;

namesTable := dataset('x',namesRecord,FLAT);

output(hallidayLibrary(namesTable).both,,named('both'));

