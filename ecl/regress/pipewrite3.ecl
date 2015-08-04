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

import lib_stringlib;
import std.str;

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'12$34'}], { string line}) : stored('nofold');

OUTPUT(d(line!='p1'),,PIPE('cmd /C more > out1.csv', output(csv)));
OUTPUT(d(line!='p2'),,PIPE('cmd /C more > out2.csv', output(csv(terminator('$'),quote('!')))));
OUTPUT(d(line!='p3'),,PIPE('cmd /C more > out1.xml', output(xml)));
OUTPUT(d(line!='p4'),,PIPE('cmd /C more > out2.xml', output(xml('Zongo', heading('<Zingo>','</Zingo>')))));
OUTPUT(d(line!='p4'),,PIPE('cmd /C more > out3.xml', output(xml('Zongo', noroot))));
OUTPUT(d(line!='p4'),,PIPE('cmd /C more > out.'+d.line[1]+'.xml', output(xml('Zongo')), repeat));
