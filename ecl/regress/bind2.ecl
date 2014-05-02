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

#onwarning ('all', error);

/*
a(integer x, integer y) := x * y;
b(integer x, integer y) := a(x, y) + a(x, y);
c(integer x, integer y) := b(x, y) + b(x, y);

d(integer x, integer y, integer z) := c(1, 2) + c(1, 2) + z;
e(integer x, integer y) := d(x, y, 8) + d(x, y, 9);
*/

f(integer x, integer y) := (x * y);
g(integer x, integer y, integer z) := f(x, y) + f(x, y) + 1;
h(integer x, integer y) := g(x, y, 8) + g(x, y, 9);

//output(person,{c(1,3)},'out.d00');    // Need to common up expansions of expansions.
//output(person,{e(1,3)},'out.d00');    // Don't expand again if already fully bound
person := dataset('person', { unsigned8 person_id, string1 per_sex; }, thor);
output(person,{h(1,3)},'out.d00');      // need to common up if parameters are insignificant
