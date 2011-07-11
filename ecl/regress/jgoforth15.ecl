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


STRING1 s1 := 'C' : stored('s1');
EBCDIC STRING sp1 := (ebcdic string)s1 : stored('sp1');
output(stringlib.data2string((data)s1));
output(stringlib.data2string((data)sp1));

STRING1 s2 := 'C' : stored('s2');
EBCDIC STRING1 sp2 := (ebcdic string)s2 : stored('sp2');
output(stringlib.data2string((data)s2));
output(stringlib.data2string((data)sp2));

STRING1 s3 := 'C';
EBCDIC STRING sp3 := (ebcdic string)s3;
output(stringlib.data2string((data)s3));
output(stringlib.data2string((data)sp3));


STRING1 s4 := 'C';
EBCDIC STRING1 sp4 := (ebcdic string1)s4;
output(stringlib.data2string((data)s4));
output(stringlib.data2string((data)sp4));

