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

do(func, value) := MACRO
 PARALLEL(
    output(func + '(' + #TEXT(value) + ')'),
    #EXPAND(func)((string)value),
    #EXPAND(func)((qstring)value),
    #EXPAND(func)((data)value)
    )
ENDMACRO;

doU(func, value) := MACRO
 PARALLEL(
    output(func + '(' + #TEXT(value) + ')'),
    #EXPAND(func)((unicode)value),
    #EXPAND(func)((utf8)value),
    #EXPAND(func)((data)value)
    )
ENDMACRO;

s1 := 'ABCD123 5' : stored('s1');
s2 := 'ABCD123 5 ' : stored('s2');

do('HASH', 'ABCD123 5');
do('HASH', s1);
do('HASH', 'ABCD123 5 ');
do('HASH', s2);
do('HASH32', 'ABCD123 5');
do('HASH32', s1);
do('HASH32', 'ABCD123 5 ');
do('HASH32', s2);
do('HASH64', 'ABCD123 5');
do('HASH64', s1);
do('HASH64', 'ABCD123 5 ');
do('HASH64', s2);

u1 := U'ABCD123 5' : stored('u1');
u2 := U'ABCD123 5 ' : stored('u2');

do('HASH', U'ABCD123 5');
do('HASH', u1);
do('HASH', U'ABCD123 5 ');
do('HASH', u2);
do('HASH32', U'ABCD123 5');
do('HASH32', u1);
do('HASH32', U'ABCD123 5 ');
do('HASH32', u2);
do('HASH64', U'ABCD123 5');
do('HASH64', u1);
do('HASH64', U'ABCD123 5 ');
do('HASH64', u2);
