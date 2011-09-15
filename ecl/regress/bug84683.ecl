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

// Works:

// le := {DATA val {MAXLENGTH(99999)}};
// DATA99999 arraye  := x'00';
// dse := DATASET([{arraye}], le);

// datadse := DATASET('~temp::dataarrayein', le, THOR);
// DATA99999 arraye1  := datadse[1].val;

// dse1 := DATASET([{arraye1}], le);


// SEQUENTIAL(output(dse, , '~temp::dataarrayein', OVERWRITE)
// , output(dse1, , '~temp::dataarraye1', OVERWRITE)
// );

// Throws a segfault:

le := {DATA val {MAXLENGTH(100000)}};
DATA100000 arraye  := x'00';
dse := DATASET([{arraye}], le);

datadse := DATASET('~temp::dataarrayein', le, THOR);
DATA100000 arraye1  := datadse[1].val;

dse1 := DATASET([{arraye1}], le);

SEQUENTIAL(
output(dse, , '~temp::dataarrayein', OVERWRITE)
, output(dse1, , '~temp::dataarraye1', OVERWRITE)
);