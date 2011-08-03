/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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

#workunit ('name', 'aaaLibrary2'); // NOTE - not yet supported in Roxie but required in hthor.
#option ('targetClusterType', 'roxie');
import std.system.thorlib;

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


filterDatasetLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(FilterDatasetInterface)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export matches := g(surname = search);
    export others := g((surname != search) and (surname != getEnv('SkipSurname', 'NoSkip')));
end;

#option ('targetClusterType', 'roxie');
build(filterDatasetLibrary);