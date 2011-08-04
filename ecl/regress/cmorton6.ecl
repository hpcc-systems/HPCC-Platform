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

#option ('targetClusterType', 'roxie');

r := record
unsigned id;
    end;
namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
dataset(r)      recs;
            END;

namesTable1 := dataset('x1',namesRecord,FLAT);

output(namesTable1);

nullTable := dataset('x1',namesRecord,FLAT)(false) : stored('x', few);

doZ := function

output(nullTable);
return (namesTable1);
end;

boolean cond := false : stored('cond');

if (cond,
parallel(
output(nullTable),
output(namesTable1)
));



