/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
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
filtered2 := LIBRARY(INTERNAL(nameFilterLibrary), FilterLibrary, createFilterLibrary('Halliday', namesTable));
output(filtered2.excluded,,named('Excluded'));
