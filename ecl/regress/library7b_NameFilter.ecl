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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

NameFilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export included := ds;
    export excluded := ds;
    export integer version;
    export integer numBoth;
    export string10 firstIncludedName;
end;

filterNames(dataset(namesRecord) ds, string search, boolean onlyOldies) := LIBRARY('NameLibrary_1_0', NameFilterLibrary(ds, search, onlyOldies));

//Use a library in global and child context

impNameFilter(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export included := g(surname != search);
    export excluded := g(surname = search);
    export version := 100;
    export numBoth := count(g);
    export firstIncludedName := included[1].forename;
end;

#workunit('name','NameFilter');
BUILD(impNameFilter);

