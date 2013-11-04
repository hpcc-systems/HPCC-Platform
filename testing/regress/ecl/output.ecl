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

d := dataset([{1, 'a', 3.4, 5.6, true, false}], {unsigned a, string1 b, real4 c, decimal5_3 de, boolean t, boolean f});

output(d);
output(d, named('named1'));

1;
'a';
3.4;
(decimal5_3) 5.6;
true;
false;

output(1);
output('a');
output(3.4);
output((decimal1_1) 5.6);
output(true);
output(false);

output(1, named('named2'));
output('a', named('named3'));
output(3.4, named('named4'));
output((decimal5_3) 5.6, named('named5'));
output(true, named('named6'));
output(false, named('named7'));
