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

#option ('childQueries', true);

Layout_T1 := RECORD
   INTEGER x1;
INTEGER x2;
STRING s3;
END;

Data_T1 := DATASET([{1,2,'a'},{1,3,'a2'},{3,5,'b'}],Layout_T1);

LAYOUT_T0 := RECORD
     INTEGER y1;
DATASET(Layout_T1) z;
END;

Data_S0 := DATASET([{3},{2},{6},{5}],{INTEGER y});

LAYOUT_T0 makeT0({INTEGER y} l) := TRANSFORM
   self.y1 := l.y;
   self.z := Data_T1;
END;

Layout_T0 filterZ(Layout_T0 l) := TRANSFORM
   self.z := l.z(l.y1 between l.z.x1 and l.z.x2);
self := l;
END;

D_1 := project(Data_S0,makeT0(LEFT));
// output(D_1);
output(project(D_1,filterZ(LEFT)));
