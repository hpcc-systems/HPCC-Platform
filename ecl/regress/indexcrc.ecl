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

idx1 := INDEX(ds1, {i}, { DATASET(chld), fp}, 'crcbug1.idx');
idx2 := INDEX(ds2, {i}, { DATASET(chld), fp}, 'crcbug2.idx');
idx3 := INDEX(ds3, {i}, { DATASET chld := chld, fp}, 'crcbug3.idx');

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

