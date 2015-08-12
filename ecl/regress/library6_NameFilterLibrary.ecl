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

INameFilter := interface
export dataset(namesRecord) included;
export dataset(namesRecord) excluded;
    end;


NameFilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface(INameFilter)
    end;

filterDataset(dataset(namesRecord) ds, string search, boolean onlyOldies) := LIBRARY('NameLibrary_1_0', NameFilterLibrary(ds, search, onlyOldies));

//Ensure only included and excluded are exported from the library
NameFilterLibrary_1_0(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(NameFilterLibrary)
    f := ds;
    export g := if (onlyOldies, f(age >= 65), f);
    export dataset(namesRecord) included := g(surname != search);
    export dataset(namesRecord) excluded := g(surname = search);
end;

#workunit('name','NameFilterLibrary_1_0');
BUILD(NameFilterLibrary_1_0);
