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

#option ('targetClusterType', 'hthor');
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


//NOTE: This is an example of inheritance!

hallidayLibrary(dataset(namesRecord) ds) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
    export grouped dataset(namesRecord) both;
end;

namesTable := dataset('x',namesRecord,FLAT);

//Library definitions

impFilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(FilterLibrary)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export included := g(surname != search);
    export excluded := g(surname = search);
end;

baseLibrary(dataset(namesRecord) ds) := LIBRARY('FilterLibrary', filterLibrary(ds, 'Hawthorn', false));
impHallidayLibrary(dataset(namesRecord) ds) := module(baseLibrary(ds)),library(HallidayLibrary)
    export both := group(included,false) + group(excluded, false);
end;

#workunit('name','HallidayLibrary');
BUILD(impHallidayLibrary);
