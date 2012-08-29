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

#stored ('useX1', true);

set of integer x1 := [1, 2, 3, 4];
set of integer x2 := [5, 6, 7, 8];

boolean useX1 := false : stored('useX1');
integer search1 := 1    : stored('search1');
integer search2 := 5    : stored('search2');

set of integer x := if(useX1, x1, x2);

search1 in x;
search2 in x;


set of integer y1 := [1, 2, 3, 4] : stored('y1');
set of integer2 y2 := [5, 6, 7, 8] : stored('y2');

set of integer y := if(useX1, y1, y2);

search1 in y;
search2 in y;


set of string z1 := ['Gavin', 'Jason', 'Emma', 'Vicky'];
set of string z2 := ['Liz', 'Rochelle', '?', 'David'];

set of string z := if(useX1, z1, z2);

string ssearch1 := 'Gavin'  : stored('ssearch1');
string ssearch2 := 'Liz'    : stored('ssearch2');

ssearch1 in z;
ssearch2 in z;



set of integer5 ya1 := [1, 2, 3, 4];
set of integer5 ya2 := [5, 6, 7, 8] : stored('ya2');

set of integer ya := if(useX1, ya1, ya2);

search1 in ya;
search2 in ya;

output(y1);

set of integer2 x3 := (set of integer1)([500, 600, 700, 800]);
output(x3);
