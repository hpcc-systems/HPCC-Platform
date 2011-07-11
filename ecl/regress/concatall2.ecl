/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
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


data10 vdata10 := D'Gavin Hall' : stored('vdata10');
string10 vstring10 := 'Gavin Hall' : stored('vstring10');
unicode10 vunicode10 := U'Gavin Hall' : stored('vunicode10');
utf8_10 vutf810 := U8'Gavin Hall' : stored('vutf810');
varunicode10 vvarunicode10 := U'Gavin Hall' : stored('vvarunicode10');

output((data)(vdata10 + D'iday'));
output((data13)(vdata10 + D'idby'));
output((data14)(vdata10 + D'idcy'));
output((data15)(vdata10 + D'iddy'));

output((string)(vstring10 + 'iday'));
output((string13)(vstring10 + 'idby'));
output((string14)(vstring10 + 'idcy'));
output((string15)(vstring10 + 'iddy'));

output((varstring)(vstring10 + 'iday'));
output((varstring13)(vstring10 + 'idby'));
output((varstring14)(vstring10 + 'idcy'));
output((varstring15)(vstring10 + 'iddy'));

output((unicode)(vunicode10 + U'iday'));
output((unicode13)(vunicode10 + U'idby'));
output((unicode14)(vunicode10 + U'idcy'));
output((unicode15)(vunicode10 + U'iddy'));


output((varunicode)(vunicode10 + U'iday'));
output((varunicode13)(vunicode10 + U'idby'));
output((varunicode14)(vunicode10 + U'idcy'));
output((varunicode15)(vunicode10 + U'iddy'));

output((utf8)(vutf810 + U8'iday'));
output((utf8_13)(vutf810 + U8'idby'));
output((utf8_14)(vutf810 + U8'idcy'));
output((utf8_15)(vutf810 + U8'iddy'));

d := dataset([U8'iday', U8'idby',U8'idcy', U8'iddy'], { utf8 value });
output(table(d, { utf8 value := vutf810 + value }));
output(table(d, { utf8_13 value := vutf810 + value }));
output(table(d, { utf8_15 value := vutf810 + value }));
output(dataset([
        (utf8)(vutf810 + U8'iday'), 
        (utf8_13)(vutf810 + U8'idby'),
        (utf8_14)(vutf810 + U8'idcy'),
        (utf8_15)(vutf810 + U8'iddy')], { utf8 value }));
