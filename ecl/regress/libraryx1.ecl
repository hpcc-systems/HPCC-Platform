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


//Define an interface to specify what the arguments to the library are
IFilterLibraryArgs := INTERFACE
    export string search;
    export dataset(namesRecord) ds;
    export boolean onlyOldies := false;         // default value
END;

//A function that can be used to create an instance of those arguments.
createFilterLibrary(string _search, dataset(namesRecord) _ds, boolean _onlyOldies = false) := module(IFilterLibraryArgs)
    export string search := _search;
    export dataset(namesRecord) ds := _ds;
    export onlyOldies := _onlyOldies;
end;


//Define the prototype of the library - input parameters and members
FilterLibrary(IFilterLibraryArgs args) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;

//An example of a wrapping function to wrap the call to the library
execNameFilterLibrary(IFilterLibraryArgs args) := LIBRARY('NameFilterLibrary', FilterLibrary, args);

//Define an instance of a library - parameter must match the declaration, and must define necessary members.
NameFilterLibrary(IFilterLibraryArgs args) := module,library(FilterLibrary)
    f := args.ds;
    shared g := if (args.onlyOldies, f(age >= 65), f);
    export included := g(surname != args.search);
    export excluded := g(surname = args.search);
end;


//namesTable := dataset('x',namesRecord,FLAT);
namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);


//Use a library - arguments are created explicitly and passed, library name supplied
args1 := module(IFilterLibraryArgs)
    export string search := 'Smith';
    export dataset(namesRecord) ds := namesTable;
end;

filtered := execNameFilterLibrary(args1);
output(filtered.included,,named('Included'));

//Use a library, but include it internally this time - a unique id will be created for the library name
//use the parameter creation function inline....
filtered2 := LIBRARY(INTERNAL(nameFilterLibrary), FilterLibrary, createFilterLibrary('Hawthorn', namesTable));
output(filtered2.excluded,,named('Excluded'));
