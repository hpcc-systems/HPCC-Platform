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

//An option for hqltest to build some other libraries (for regression testing)
#workunit('name','NameFilterLibrary');
BUILD(NameFilterLibrary);
