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

//  Misc  ---
h3.radsToDegs(h3.degsToRads(42));
h3.hexAreaKm2(12);
h3.hexAreaM2(12);
h3.numHexagons(4);
