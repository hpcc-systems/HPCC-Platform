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

//nothor
//nothorlcr

//Example of nested 
namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

HallidayLibrary(dataset(namesRecord) ds) := interface
    export dataset(namesRecord) included;
    export dataset(namesRecord) excluded;
end;

namesTable := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Jones','John', 44},
        {'Smith','George',75},
        {'Smith','Baby', 2}], namesRecord);

output(LIBRARY('aaaLibrary3b', HallidayLibrary(namesTable)).included,,named('Included'));

