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

#option ('optimizeDiskSource', true);
#option ('countFile', false);
#option ('targetClusterType', 'roxie');

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset('x1',namesRecord,FLAT);
output(count(namesTable1) <= 5);
namesTable2 := dataset('x2',namesRecord,FLAT);
output(count(namesTable2) < 5);
namesTable3 := dataset('x3',namesRecord,FLAT);
output(count(namesTable3) > 5);
namesTable4 := dataset('x4',namesRecord,FLAT);
output(count(namesTable4) >= 5);
namesTable5 := dataset('x5',namesRecord,FLAT);
output(count(namesTable5) = 5);
namesTable6 := dataset('x6',namesRecord,FLAT);
output(count(namesTable6) != 5);
namesTable7 := dataset('x7',namesRecord,FLAT);
output(count(namesTable7) != 5);
output(count(namesTable7) > 0);
