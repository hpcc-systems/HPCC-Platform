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

d := dataset([{'1'}, {'2'}, {'4'}], { unsigned1 x});

d1 := d(x=1);
d2 := d(x=2);
d3 := d(x=3);
d4 := d(x=4);

o1 := output(d1);
o2 := output(d2);
o3 := output(d3);
o4 := output(d4);

string1 s := '1' : stored('s');
integer8 i := 2 : stored('i');

case(s, '1'=>o1, '2'=>o2, '3'=>o3, o4);
output(case(s, '1'=>d1, '2'=>d2, '3'=>d3, d4));

case(i, 1=>o1, 2=>o2, 3=>o3, o4);
output(case(i, 1=>d1, 2=>d2, 3=>d3, d4));

UNSIGNED6 Score(STRING level) := CASE(level,
     'medium2' => 91,
     'medium'  => 90,
     'high'    => 98,
     55);

string low := 'low' : stored('low');
string medium := 'medium' : stored('medium');
string high := 'high' : stored('high');
string none := 'none' : stored('none');

low;    Score(low);
medium; Score(medium);
high;   Score(high);
none;   Score(none);
