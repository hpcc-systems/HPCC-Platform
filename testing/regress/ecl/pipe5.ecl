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

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { string line}) : stored('nofold');

p1 := PIPE(d, 'sort', { string lout }, csv, output(xml));
p2 := PIPE(d, 'sort', { string lout }, csv, output(xml(noroot)));
p3 := PIPE(d, 'sort', { string lout }, csv, output(xml), repeat);
p4 := PIPE(d, 'sort', { string lout }, csv, output(xml(noroot)), repeat);

p1a := p1(lout not in ['<Dataset>','</Dataset>']);  // strip so independent of number of nodes

output(p1a, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string l := Str.FindReplace(lout, '\r', ' ') } );


