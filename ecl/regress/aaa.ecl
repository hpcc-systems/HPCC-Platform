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

dsfile := 'aaa.d00';
idxfile := 'aaa.idx';

myRecord := record
unsigned2       extra;
            end;

tt := DATASET([{1, 2}, {1, 256}], {UNSIGNED2 id, myRecord extra});

op := OUTPUT(tt, , dsfile, OVERWRITE);

ds := DATASET(dsfile, {tt, UNSIGNED8 fp{virtual(fileposition)}}, FLAT);

idx := INDEX(ds, {id}, {extra, fp}, idxfile);

bld := BUILD(idx, OVERWRITE);

mk := SEQUENTIAL(op, bld);

mk;


output(idx);
