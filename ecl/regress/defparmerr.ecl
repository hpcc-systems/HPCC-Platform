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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

// wrong def parm
attr := 1;
m0( t = 1) := MACRO 1 endmacro;
m1( t = attr) := MACRO 1 endmacro;
m2( t = 1+2) := macro 1 endmacro;
m3( t = person) := macro 1 endmacro;
m4( t = person.per_first_name) := macro 1 endmacro;

// check for error recovery
m0();
m1();
m2();
m3();
m4();

export myMacro(t1, t2 = '2 + 3') :=  macro
    t1+t2
  endmacro;

// too many parms
mymacro(1,2,3);

// no def value
mymacro();
mymacro(,);
mymacro(,2);

