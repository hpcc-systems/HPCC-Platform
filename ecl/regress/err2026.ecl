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

aaa := DATASET('aaa', {STRING1 f1;}, THOR);
bbb := GROUP(aaa, ALL);


s := ' abc ';
TRIM(s, ALL);


f(SET is, INTEGER i) := i IN is;

MyJobSet := [1,2,3];

BOOLEAN ok := f(ALL, 1);


SORT(aaa, ALL);
