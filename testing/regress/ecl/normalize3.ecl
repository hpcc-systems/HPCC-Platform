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

import $.setup.sq;

o1 := normalize(sq.NamesTable1, left.books, transform(right));
o2 := normalize(sq.NamesTable1, sort(left.books, -rating100, +author), transform(right));
o3 := normalize(sq.NamesTable1, sort(left.books, +author, +name), transform({string name, string author}, self := right));

g1 := group(sq.NamesTable1, surname, forename);
o4 := table(g1, { cnt := count(group), numBooks := sum(group, count(books)) });     // show there is a zero size group
o5 := normalize(g1, left.books, transform(right));                                  // grouped normalize - including an "empty" group
o6 := table(o5, { cnt := count(group) });                                           // ensure the grouping worked correctly
o7 := choosen(o5, 2);                                                               // premature finish reading.

sequential(
    output(o1),
    output(o2),
    output(o3),
    output(o4),
    output(o5),
    output(o6),
    output(o7),
    output('done')
);
