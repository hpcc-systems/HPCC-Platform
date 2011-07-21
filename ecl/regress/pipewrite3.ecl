/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
