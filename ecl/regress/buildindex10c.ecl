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

#option ('sortIndexPayload', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

i1 := index(namesTable, { age }, {surname, forename}, 'i1');                //NB: Default for whether this is handled as sorted is not dependent on sortIndexPayload setting
i2 := index(namesTable, { age }, {surname, forename}, 'i1', sort all);
i3 := index(namesTable, { age }, {surname, forename}, 'i1', sort keyed);

integer x := 0 : stored('x');

case(x,
1=>build(i1),
2=>build(i2),
3=>build(i3)
);

output(sort(sorted(i1), age));                              // should sort
output(sort(sorted(i2), age));                              // should sort
output(sort(sorted(i3), age));                              // should not sort
output(sort(sorted(i1), age, surname, forename));       // should not sort
output(sort(sorted(i2), age, surname, forename));       // should not sort
output(sort(sorted(i3), age, surname, forename));       // should sort
