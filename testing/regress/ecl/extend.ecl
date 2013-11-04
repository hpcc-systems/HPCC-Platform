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
