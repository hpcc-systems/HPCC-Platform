/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//class=3rdparty

IMPORT lib_h3;

h3 := lib_h3.h3;

res := 10;
lat := 40.689167;
lng := -74.044444;

//  Indexing  ---
h3Idx := h3.index(lat, lng, res);
h3Idx;
h3.center(h3Idx);
h3.boundary(h3Idx);

//  Introspection  ---
h3.resolution(h3Idx);
h3.baseCell(h3Idx);
strIdx := h3.toString(h3Idx);
strIdx;
h3.fromString(strIdx) = h3Idx;
h3.isValid(h3Idx) = true;
h3.isValid(42) = false;
h3.toString(h3.parent(h3Idx, res - 1));
h3.toString(h3.parent(h3Idx, res - 2));
h3.toString(h3.parent(h3Idx, res - 3));
h3.toString(h3.parent(h3Idx, res - 4));

//  Traversal  ---
kRing := h3.kRing(h3Idx, 1);
kRing;
COUNT(kRing);
hexRing := h3.hexRing(h3Idx, 1);
hexRing;
COUNT(hexRing);
h3.distance(h3Idx, hexRing[1]);

//  Hierarchy  ---
children := h3.children(h3Idx, res + 1);
children;
compact := h3.compact(children);
h3.uncompact(compact, res);

//  Regions ---;
poly := h3.polyfill(DATASET([{40.5, -74.5}, {40.5, -64.5}, {30.5, -64.5}, {30.5, -74.5}], h3_point_t), 4);
COUNT(poly);
poly;

//  Misc  ---
h3.radsToDegs(h3.degsToRads(42));
h3.hexAreaKm2(12);
h3.hexAreaM2(12);
h3.numHexagons(4);
