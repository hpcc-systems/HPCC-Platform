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
    STRING1 Value1;
    STRING2 Value2;
END;
MyRec2 := RECORD
    STRING1 Value1;
    STRING2 Value3;
END;

LeftFile := DATASET([
           {'C','A1'},
           {'C','A2'},
           {'C','A3'},
           {'C','A4'},
           {'C','A5'},
     {'X','B1'},
     {'A','C1'}],MyRec);

RightFile := DATASET([
            {'C','X1'},
            {'C','X2'},
            {'C','X3'},
            {'C','X4'},
            {'C','X5'},
      {'B','Y'},
      {'A','Z'}],MyRec2);

leftfile tr(leftfile L, leftfile r) := TRANSFORM
    SELF.value1 := L.value1;
    SELF.value2 := R.value2;
  END;

JoinedRecs1 := JOIN(LeftFile,RightFile,
            LEFT.Value1 = RIGHT.Value1,atmost(3));
JoinedRecs2 := JOIN(LeftFile,RightFile,
            LEFT.Value1 = RIGHT.Value1,limit(3,skip));

JoinedRecs3 := JOIN(LeftFile,LeftFile,
            LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),atmost(3));
JoinedRecs4 := JOIN(LeftFile,LeftFile,
            LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),limit(3,skip));

JoinedRecs1a := JOIN(LeftFile,RightFile,
            LEFT.Value1 = RIGHT.Value1,atmost(3), LEFT OUTER);
JoinedRecs2a := JOIN(LeftFile,RightFile,
            LEFT.Value1 = RIGHT.Value1,limit(3,skip), LEFT OUTER);

JoinedRecs3a := JOIN(LeftFile,LeftFile,
            LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),atmost(3), LEFT OUTER);
JoinedRecs4a := JOIN(LeftFile,LeftFile,
            LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),limit(3,skip), LEFT OUTER);

JoinedRecs1b := JOIN(LeftFile,RightFile,
            LEFT.Value1 = RIGHT.Value1,atmost(3), LEFT ONLY);
// ***codegen disallows combination of limit with only***
//JoinedRecs2b := JOIN(LeftFile,RightFile,
//          LEFT.Value1 = RIGHT.Value1,limit(3,skip), LEFT ONLY);

JoinedRecs3b := JOIN(LeftFile,LeftFile,
            LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),atmost(3), LEFT ONLY);
//JoinedRecs4b := JOIN(LeftFile,LeftFile,
//          LEFT.Value1 = RIGHT.Value1,tr(LEFT,RIGHT),limit(3,skip), LEFT ONLY);

OUTPUT(sort(JoinedRecs1, record));
OUTPUT(sort(JoinedRecs2, record));
OUTPUT(sort(JoinedRecs3, record));
OUTPUT(sort(JoinedRecs4, record));

OUTPUT(sort(JoinedRecs1a, record));
OUTPUT(sort(JoinedRecs2a, record));
OUTPUT(sort(JoinedRecs3a, record));
OUTPUT(sort(JoinedRecs4a, record));

OUTPUT(sort(JoinedRecs1b, record));
//OUTPUT(sort(JoinedRecs2b, record));
OUTPUT(sort(JoinedRecs3b, record));
//OUTPUT(sort(JoinedRecs4b, record));
