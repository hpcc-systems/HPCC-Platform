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

//BUG: #14105 - duplicate
export namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

ds := dataset('x1',namesRecord,FLAT);

outr := record
string20        surname;
string10        forename;
        end;

export ds1 := project(ds, transform(outr, SELF := LEFT));

ds := dataset('x2',namesRecord,FLAT);

outr := record
string20        surname;
integer2        age := 25;
        end;

export ds2 := project(ds, transform(outr, SELF := LEFT));


combined := record
dataset(recordof(ds1))  ds1;
dataset(recordof(ds2))  ds2;
dataset(recordof(ds1))  ds1b;
dataset(recordof(ds2))  ds2b;
            end;



result := dataset([transform(combined, self.ds1 := ds1; self.ds2 := ds2; self.ds1b := []; self.ds2b := [])]);

output(result,,'out.d00');
