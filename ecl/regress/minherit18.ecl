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

//Example of nested
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;


filterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := module
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export included := g(surname != search);
    export excluded := g(surname = search);
end;


hallidayLibrary(dataset(namesRecord) ds) := module(filterLibrary(ds, 'Hawthorn', false))
    export both := group(included,false) + group(excluded, false);
end;

namesTable := dataset('x',namesRecord,FLAT);

output(hallidayLibrary(namesTable).both,,named('both'));

