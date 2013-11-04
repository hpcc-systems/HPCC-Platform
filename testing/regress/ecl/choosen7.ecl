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

rec := { string name };

ds := group(dataset(['Gavin','Nigel'], rec), name);

chooseEmpty := true : stored('chooseEmpty');


x := if(chooseEmpty, ds(false), ds);

y := choosen(nofold(x), 10);

z1 := table(y, { count(group) });
output(z1);

z2 := table(x, { count(group) });
output(z2);

xx := if(not chooseEmpty, ds(false), ds);

yy := choosen(nofold(xx), 10);

zz1 := table(yy, { count(group) });
output(zz1);

zz2 := table(xx, { count(group) });
output(zz2);
