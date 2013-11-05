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
    STRING1  Value1;
    STRING1  Value2;
    INTEGER1 Value3;
END;
SomeFile := DATASET([{'C','G',1},
                     {'C','C',2},
                     {'A','X',3},
                     {'B','G',4},
                     {'A','B',5}],MyRec);
MyOutRec := RECORD
    SomeFile.Value1;
    GrpCount := COUNT(GROUP);
    GrpSum   := SUM(GROUP,SomeFile.Value3);
END;

MyTable := TABLE(SomeFile,MyOutRec,Value1);

OUTPUT(SORT(MyTable,value1));
/* MyTable result set is:
    Rec#    Value1  GrpCount    GrpSum
    1       A       2           8
    2       B       1           4
    3       C       2           3
*/
