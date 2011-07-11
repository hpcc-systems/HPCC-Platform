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

#option ('targetService', 'aaaLibrary3b')
#option ('createServiceAlias', true)

//Example of nested 
namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
    export unsigned someValue;      // different interface from 1.2 in library2
end;


HallidayLibrary(dataset(namesRecord) ds) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;


HallidayLibrary3(dataset(namesRecord) ds) := module,library(HallidayLibrary)
    shared baseLibrary := LIBRARY('aaaLibrary3a', filterLibrary(ds, 'Halliday', false));
    export included := baseLibrary.included;
    export excluded := baseLibrary.excluded;
end;

BUILD(HallidayLibrary3);
