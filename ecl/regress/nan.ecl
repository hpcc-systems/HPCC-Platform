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

REAL8 createNan(unsigned4 which = 1) := BEGINC++
    union
    {
        unsigned l;
        float f;
    } u;

    u.l = 0x7F800000 | (which & 0x07FFFFF);
    return u.f;
ENDC++;

boolean isNan(REAL x) := BEGINC++
    union
    {
        unsigned l;
        float f;
    } u;

    u.f = x;
    return (u.l & 0x7F800000) == 0x7F800000;
ENDC++;

unsigned4 whichNan(REAL x) := BEGINC++
    union
    {
        unsigned l;
        float f;
    } u;

    u.f = x;
    if ((u.l & 0x7F800000) == 0x7F800000)
        return u.l & 0x7FFFFF;
    return 0;
ENDC++;

z1 := createNan(1);
z2 := createNan(2);
output(isNan(2)); '\n';
output(isNan(z1));'\n';
output(isNan(z1*2.0));'\n';
output(z1);'\n';
output(z1*2.0);'\n';
output(whichNan(z1));'\n';
output(whichNan(z2)); '\n';
output(whichNan(z1*z2) );'\n';
