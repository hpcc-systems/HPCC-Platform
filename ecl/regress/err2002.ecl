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

WAIT('STR');
v1 := 10.2 % 11.2;
v2 := 10.2 DIV 11.2;
v3 := 10.2 | 11.2;
v4 := 10.2 & 11.2;
v5 := 10.2 ^ 11.2;
STRING s := 'This is a string. ';
s[0.23];
s[0.23..2.34];
s[..2.3];
s[2.3..];
INTFORMAT(1.2, 2.2, 3.3);
REALFORMAT(1.0, 2.0, 3.0);
CHOOSE(3.2, 9, 8, 7, 6, 5);

aaa := DATASET('aaa',{STRING1 fa; }, FLAT);

OUTPUT(choosen(aaa,10.2));
OUTPUT(DEDUP(aaa, fa = 'A', KEEP 2.2));
