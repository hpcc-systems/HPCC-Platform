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
string20        field1;
unicode20       field2;
unicode20       field3;
string20        field4;
string20        field5;
            END;

inputTable := dataset('~gavin::input',accountRecord,CSV(HEADING(1),SEPARATOR([',','==>']),QUOTE(['\'','"','$$']),TERMINATOR(['\r\n','\r','\n'])));
output(inputTable,{field1,field2,field3,field4,field5,'\r\n'},'gavin::output');
