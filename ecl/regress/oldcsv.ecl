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

export dstring(string del) := TYPE
    export integer physicallength(string s) := StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(string s) := s[1..StringLib.StringUnboundedUnsafeFind(s,del)-1];
    export string store(string s) := s+del; // Untested (vlength output generally broken)
END;

accountRecord :=
            RECORD
dstring(',')    field1;
dstring(',')    field2;
dstring(',')    field3;
dstring(',')    field4;
dstring('\r\n') field5;
            END;

inputTable := dataset('~gavin::input',accountRecord,THOR);
output(inputTable,{(string20)field1,(string4)LENGTH((string)field2),(string20)field2,(string20)field3,(string20)field4,(string20)field5,'\r\n'},'gavin::output');
