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

//UseStandardFiles
// straight disk count

string c1 := 'CLAIRE' : stored('c1');
string c2 := 'CLAIRE' : stored('c2');
string c3 := 'CLAIRE' : stored('c3');
string c4 := 'CLAIRE' : stored('c4');

recordof(DG_FlatFile) createError := TRANSFORM
    SELF.DG_firstname := 'LIMIT EXCEEDED';
    SELF := [];
END;

o1 := output(LIMIT(DG_FlatFile(DG_firstname=c1), 2, skip)) : independent;
o2 := output(LIMIT(DG_FlatFile(DG_firstname=c2), 2, ONFAIL(createError))) : independent;
o3 := count(LIMIT(DG_FlatFile(DG_firstname=c3), 2, skip)) : independent;
o4 := count(LIMIT(DG_FlatFile(DG_firstname=c4), 2, ONFAIL(createError))) : independent;

o1; o2; o3; o4;

