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

#option ('targetService', 'aaaLibrary5')
#option ('createServiceAlias', true)

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, string search) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


filterDatasetLibrary(dataset(namesRecord) ds, string search) := module,library(FilterDatasetInterface)
    shared f := ds;
    export matches := if (count(f(surname = search)) > 0, f);
    export others := if (count(f(surname = search)) <= 0, f);
end;

build(filterDatasetLibrary);