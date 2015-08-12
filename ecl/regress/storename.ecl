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

x := 'on' : stored('x');

/*
y := dataset(x, { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

output(y);


z := index(y, { id, filepos }, 'ix' + x);

output(z(id > 10));


y2 := dataset('inxx'+, { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

output(y);


z := index(y, { id, filepos }, 'ix' + x);

output(z(id > 10));
*/

string2 previous_file := '' : stored('AccessPreviousFile');
boolean prev_file := previous_file = 'on';

export version :=
    if(prev_file,
        '20021031',
        '20021125');

infile := dataset('in', { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);


y := dataset('zz', { unsigned4 id, unsigned8 filepos{virtual(fileposition)} }, thor);

z2 := index(y, { id, filepos }, 'ix' + version);

infile t(infile l) := transform
    self := l;
    end;

a := join(infile, y, left.id = right.id,t(left),keyed(z2));

output(a);
