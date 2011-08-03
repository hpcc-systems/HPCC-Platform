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

export display := SERVICE
 echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;

namesRecord :=
            RECORD
unsigned4       holeid;
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset('base1',namesRecord,THOR);
namesTable2 := dataset('base2',namesRecord,THOR);
namesTable3 := dataset('base3',namesRecord,THOR);
namesTable4 := dataset('base4',namesRecord,THOR);
namesTable5 := dataset('base5',namesRecord,THOR);

count(namesTable1);
count(namesTable1);
count(namesTable1) + count(namesTable1);
count(namesTable2) + count(namesTable3);
count(namesTable2) + count(namesTable3);

display.echo('Total records = ' + (string)count(namesTable4));
