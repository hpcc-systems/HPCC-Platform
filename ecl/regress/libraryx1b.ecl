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


//Define the prototype of the library - input parameters and members
FilterLibrary(string search, dataset(namesRecord) ds, boolean onlyOldies) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;

//Define an instance of a library - parameter must match the declaration, and must define necessary members.
NameFilterLibrary(string search, dataset(namesRecord) ds, boolean onlyOldies) := module,library(FilterLibrary)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export included := g(surname != search);
    export excluded := g(surname = search);
end;


//namesTable := dataset('x',namesRecord,FLAT);
namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

//Use a library - arguments are passed, library name supplied
filtered := LIBRARY('NameFilterLibrary', FilterLibrary('Smith', namesTable, false));
output(filtered.included,,named('Included'));

//Use a library, but include it internally this time - a unique id will be created for the library name
filtered2 := LIBRARY(INTERNAL(NameFilterLibrary), FilterLibrary('Hawthorn', namesTable, false));
output(filtered2.excluded,,named('Excluded'));
