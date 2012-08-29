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

//Number of leading spaces will force the lines to come out in the correct order
d := dataset(['</Zingo>', ' <Zango>Line3</Zango>', '  <Zango>Middle</Zango>', '   <Zango>Line1</Zango>', '    <Zingo>' ], { string line}) : stored('nofold');

#IF (__OS__ = 'windows')
pipeCmd := 'sort';
#ELSE
pipeCmd := 'sh -c \'export LC_ALL=C; sort\'';
#END
p1 := PIPE(d(line!='p1'), pipeCmd, { string lout{XPATH('')} }, xml('Zingo/Zango'), output(csv));

output(p1, { string l := Str.FindReplace(lout, '\r', ' ') } );


