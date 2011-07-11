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

MyRec := RECORD
    STRING2 Value1;
    STRING1 Value2;
END;

File1 := DATASET([{'A1','A'},
                 {'A2','A'},
                 {'A3','B'},
                 {'A4','B'},
                 {'A5','C'}],MyRec);

File2 := DATASET([{'B1','A'},
                 {'B2','A'},
                 {'B3','B'},
                 {'B4','B'},
                 {'B5','C'}],MyRec);

o1 := output(File1, NAMED('testResult'), EXTEND);
o2 := output(File2, NAMED('testResult'), EXTEND);
o3 := output(dataset(WORKUNIT('testResult'), MyRec));

SEQUENTIAL(o1, o2, o3);
