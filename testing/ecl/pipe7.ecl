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

d1 := dataset(['</Dataset>', ' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>', '    <Dataset>' ], { string line }) : stored('nofold');
d2 := dataset([' <Row>Line3</Row>', '  <Row>Middle</Row>', '   <Row>Line1</Row>'], { string line }) : stored('nofold2');
d3 := dataset([' <Dataset><Row>Line3</Row></Dataset>', '  <Dataset><Row>Middle</Row></Dataset>', '   <Dataset><Row>Line1</Row></Dataset>'], { string line }) : stored('nofold3');

#IF (__OS__ = 'windows')
pipeCmd := 'sort';
#ELSE
pipeCmd := 'sh -c \'export LC_ALL=C; sort\'';
#END

p1 := PIPE(d1, pipeCmd, { string lout{XPATH('')} }, xml('Dataset/Row'), output(csv));
p2 := PIPE(d2, pipeCmd, { string lout{XPATH('')} }, xml(noroot), output(csv));
p3 := PIPE(d3, pipeCmd, { string lout{XPATH('')} }, xml('Dataset/Row'), output(csv), repeat);
p4 := PIPE(d2, pipeCmd, { string lout{XPATH('')} }, xml('Row', noroot), output(csv), repeat);

output(p1, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string l := Str.FindReplace(lout, '\r', ' ') } );


