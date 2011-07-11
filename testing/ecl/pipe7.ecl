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

import lib_stringlib;
import std.str;

d1 := dataset(['</Dataset>', ' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>', '    <Dataset>' ], { string line }) : stored('nofold');
d2 := dataset([' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>'], { string line }) : stored('nofold2');
d3 := dataset([' <Dataset><Row>Line3</Row></Dataset>', '  <Dataset><Row>Middle</Row></Dataset>', '   <Dataset><Row>Line1</Row></Dataset>'], { string line }) : stored('nofold3');

p1 := PIPE(d1, 'sort', { string lout{XPATH('')} }, xml('Dataset/Row'), output(csv));
p2 := PIPE(d2, 'sort', { string lout{XPATH('')} }, xml(noroot), output(csv));
p3 := PIPE(d3, 'sort', { string lout{XPATH('')} }, xml('Dataset/Row'), output(csv), repeat);
p4 := PIPE(d2, 'sort', { string lout{XPATH('')} }, xml('Row', noroot), output(csv), repeat);

output(p1, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string l := Str.FindReplace(lout, '\r', ' ') } );


