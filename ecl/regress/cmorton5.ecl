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


export namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

export processFile(dataset(namesRecord) dsIn) := function

dsOut := dsIn(surname <> '');

failed := exists(dsIn) and ~ exists(dsOut);

if (failed, fail('Oh dear!' + (string)count(dsIn)));

return dsOut;
end;


namesTable := dataset('x',namesRecord,FLAT);

x := processFile(namesTable);


namesRecord t(x l) := transform

    SELF := l;
    end;


outRec := record
unsigned3 id;
        end;

d2 := dataset([{(unsigned3) count(x)}], outRec);

output(d2);
