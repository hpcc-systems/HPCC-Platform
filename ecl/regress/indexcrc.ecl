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

crec := RECORD
    UNSIGNED1 j;
    STRING x;
    STRING y;
END;

crec_combined := RECORD
    UNSIGNED1 j;
    STRING z;
END;

prec := RECORD
    INTEGER1 i;
    DATASET(crec) chld;
END;

prec_mod := RECORD
    INTEGER4 i;
    DATASET(crec) chld;
END;

prec_combined := RECORD
    INTEGER1 i;
    DATASET(crec_combined) chld;
END;

baseds := DATASET([{1, [{99, 'fool', 'hardy'}, {101, 'for', 'tune'}]}, {2, [{33, 'house', 'brick'}, {66, 'pole', 'vault'}]}], prec);

crec_combined cxfm_combine(crec l) := TRANSFORM
    SELF.j := l.j;
    SELF.z := l.x + l.y;
END;

prec_combined pxfm_combine(prec l) := TRANSFORM
    SELF.chld := PROJECT(l.chld, cxfm_combine(LEFT));
    SELF := l;
END;

prec_mod pxfm_mod(prec l) := TRANSFORM
    SELF := l;
END;

ds1 := DATASET('crcbug1.d00', {prec, UNSIGNED8 fp{virtual(fileposition)}}, FLAT);
ds2 := DATASET('crcbug2.d00', {prec_combined, UNSIGNED8 fp{virtual(fileposition)}}, FLAT);
ds3 := DATASET('crcbug3.d00', {prec_mod, UNSIGNED8 fp{virtual(fileposition)}}, FLAT);

idx1 := INDEX(ds1, {i}, {DATASET chld := chld, fp}, 'crcbug1.idx');
idx2 := INDEX(ds2, {i}, {DATASET chld := chld, fp}, 'crcbug2.idx');
idx3 := INDEX(ds3, {i}, {DATASET chld := chld, fp}, 'crcbug3.idx');

SEQUENTIAL(
    OUTPUT(baseds, , 'crcbug1.d00', OVERWRITE),
    BUILD(idx1, OVERWRITE),
    OUTPUT(idx1(i=1)),

    OUTPUT(PROJECT(baseds, pxfm_combine(LEFT)), , 'crcbug2.d00', OVERWRITE),
    BUILD(idx2, OVERWRITE),
    OUTPUT(idx2(i=1)),

    OUTPUT(PROJECT(baseds, pxfm_mod(LEFT)), , 'crcbug3.d00', OVERWRITE),
    BUILD(idx3, OVERWRITE),
    OUTPUT(idx3(i=1))
);

