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
