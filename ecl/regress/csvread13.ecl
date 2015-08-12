/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

accountRecord :=
            RECORD
string      field1;
string      field2;
string      field3;
string      field4;
string      field5;
            END;

allRecord :=
            RECORD
string      field;
       END;

allRecord t(accountRecord l) := TRANSFORM
    SELF.field :=  l.field1 + '*' + l.field2 + '*' + l.field3 + '*' + l.field4 + '*' + l.field5 + TRANSFER(l, STRING10);
END;

inputTable := dataset('~file::127.0.0.1::temp::csvread.csv',accountRecord,CSV(HEADING(1),SEPARATOR([',','==>']),QUOTE(['\'','"','$$']),TERMINATOR(['\r\n','\r','\n'])));
output(PROJECT(inputTable,t(LEFT),KEYED));
