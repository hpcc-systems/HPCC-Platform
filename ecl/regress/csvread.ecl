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

#option ('optimizeGraph', false);
#option ('foldAssign', false);

/* Demo hql to test reading CSV */
/*
Input:

***This is a header Line***
f1,f2,  f3,f4,f5
f1,,f3,,f5
f1,f2,
$$f1$$,$$f2$$
,,,f4
none,"",$$$$,empty,''
=>==>=>==>f3===>
"f,1","f==>2","f'3"
x'==>',2','a,,'
'One ','Two',' Three '
"Gavin"and"Mia",Gavin"and"Mia

Expected:

f1 f2 f3 f4 f5
f1    f3    f5
f1 f2
f1 f2
         f4
none     empty
=> => f3=
f,1 f==>2 f'3
x'  ',2' 'a,,
*/

accountRecord :=
            RECORD
string      field1;
string      field2;
string      field3;
string      field4;
string      field5;
            END;

inputTable := dataset('~file::127.0.0.1::temp::csvread.csv',accountRecord,CSV(HEADING(1),SEPARATOR([',','==>']),QUOTE(['\'','"','$$']),TERMINATOR(['\r\n','\r','\n'])));
output(inputTable,{field1,'*',field2,'*',field3,'*',field4,'*',field5,'\r\n'},'gavin::output',overwrite);

inputTable2 := dataset('~file::127.0.0.1::temp::csvread.csv',accountRecord,CSV(HEADING(1),SEPARATOR([',','==>']),QUOTE(['\'','"','$$']),TERMINATOR(['\r\n','\r','\n']), NOTRIM));
output(inputTable2,{field1,'*',field2,'*',field3,'*',field4,'*',field5,'\r\n'},'gavin::output2',overwrite);
