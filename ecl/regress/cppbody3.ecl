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

// check that crcs are recalculated for C++ blocks (bug #17873)


real8 s1 :=
BEGINC++
return sqrt(2.0);
ENDC++;
s1p := s1 : persist('s1');
output(s1p);

real8 s2 :=
BEGINC++
return sqrt(2.0);
ENDC++;
s2p := s2 : persist('s2');
output(s2p);

real8 s3 :=
BEGINC++
return sqrt(3.0);
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

