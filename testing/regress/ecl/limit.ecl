/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
