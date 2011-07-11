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
"Gavin"and"Liz",Gavin"and"Liz

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
