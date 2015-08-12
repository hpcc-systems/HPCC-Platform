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

d := dataset('in', { string15 name, unsigned value }, thor);

i := index(d, { name, value } , {}, '\\home\\person.name_first.key');

f := i(name != 'Gavin');

r1 := RECORD
string15 name;
unsigned value;
unsigned seq;
  END;
  
p := PROJECT(f, TRANSFORM(r1, SELF.seq := RANDOM(), SELF := LEFT));
s1 := SORT(p, value);

output(s1(name != 'Jim'));

p2 := DEDUP(s1, name);

output(p2);

f2 := JOIN([s1,p2,p2], LEFT.value = RIGHT.value, TRANSFORM(LEFT), SORTED(value));

output(f2);
