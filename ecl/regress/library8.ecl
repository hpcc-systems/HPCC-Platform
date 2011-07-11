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
unsigned6       did;
string20        surname;
string10        forename;
integer2        age := 25;
string100       extra;
            END;

slimRecord := 
            RECORD
unsigned6       did;
string20        surname;
string10        forename;
            END;


processNameLibrary(dataset(namesRecord) ds) := interface
    export dataset(slimRecord) slim;
    export dataset(namesRecord) matches;
    export dataset(namesRecord) mismatches;
end;


oldNamesTable := dataset('oldNames',namesRecord,FLAT);
newNamesTable := dataset('newNames',namesRecord,FLAT);

processedOld := LIBRARY('ProcessNameLibrary', processNameLibrary(oldNamesTable));
processedNew := LIBRARY('ProcessNameLibrary', processNameLibrary(newNamesTable));


allSlim := processedOld.slim + processedNew.slim;           // record is based on input parameters, so still not full bound before normalization....

output(allSlim);

