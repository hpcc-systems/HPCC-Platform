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

import lib_stringlib;
import std.str;

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { varstring20 line}) : stored('nofold');

dt := TABLE(d, { string l := line });

p1 := PIPE(dt(l!='p1'), 'sort', { string lout }, csv, output(csv));
p2 := PIPE(dt(l!='p2'), 'sort', { string lout }, csv, output(csv), REPEAT);
p3 := CHOOSEN(PIPE(dt(l!='p3'), 'sort', { string lout }, csv, output(csv)), 1);
p4 := CHOOSEN(PIPE(dt(l!='p4'), 'sort', { string lout }, REPEAT, csv, output(csv)), 1);

output(p1, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string20 l := Str.FindReplace(lout, '\r', ' ') } );


