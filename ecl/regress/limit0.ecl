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

#option ('globalFold', false);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
unsigned8       filepos{virtual(fileposition)}
            END;

string searchNameLow := 'Hawthorn' : stored('SearchNameLow');
string searchNameHigh := 'Hawthorn' : stored('SearchNameHigh');
string20 searchNameLow20 := 'Hawthorn' : stored('SearchNameLow20');
string20 searchNameHigh20 := 'Hawthorn' : stored('SearchNameHigh20');

namesTable := index(namesRecord,'x');

x := limit(namesTable(surname >= searchNameLow and surname < searchNameHigh), 0);

output(x);
namesTable2 := dataset('x2',namesRecord,FLAT);

y := limit(namesTable2(surname >= searchNameLow20 and surname < searchNameHigh20), 0, keyed);

output(y);
