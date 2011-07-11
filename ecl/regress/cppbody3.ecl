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

// check that crcs are recalculated for C++ blocks (bug #17873)


real8 s1 := 
BEGINC++
sqrt(2.0)
ENDC++;
s1p := s1 : persist('s1');
output(s1p);

real8 s2 := 
BEGINC++
sqrt(2.0)
ENDC++;
s2p := s2 : persist('s2');
output(s2p);

real8 s3 := 
BEGINC++
sqrt(3.0)
ENDC++;
s3p := s3 : persist('s3');
output(s3p);


export dummy := 1;

real8 s1f(real a) := 
BEGINC++
return sqrt(a);
ENDC++;
s1fp := s1f(3) : persist('s1f');
output(s1fp);

export dummy2 := 1;

real8 s1f(real a) := 
BEGINC++
return sqrt(a);
ENDC++;
s1fp := s1f(3) : persist('s2f');
output(s1fp);

export dummy3 := 1;

real8 s1f(real a) := 
BEGINC++
return sqrt(a)+1;
ENDC++;
s1fp := s1f(3) : persist('s3f');
output(s1fp);

