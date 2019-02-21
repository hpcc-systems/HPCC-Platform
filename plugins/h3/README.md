H3 Plugin
================

This plugin exposes a hexagonal hierarchical geospatial indexing system to ECL.  It is a wrapper around Ubers H3 library:
* [H3: Uber’s Hexagonal Hierarchical Spatial Index](https://eng.uber.com/h3/)
* [Documentation](https://uber.github.io/h3)


Installation and Dependencies
------------------------------

The h3 plugin has a dependency on https://github.com/uber/h3 which has been added to the HPCC-Platform repository as a git submodule.  To install:
```c
git submodule update --init --recursive
```

Quick Start
------------

Import the h3 plugin library and calculate a H3 index at the specified lat/long and at maximum resolution (15):
```c
IMPORT h3 from lib_h3; 

h3_index := h3.index(40.689167, -74.044444, 15);
```

API
----

### Type Declarations
```c
EXPORT h3_index_t := UNSIGNED8;
EXPORT h3_stringIndex_t := VARSTRING;
EXPORT h3_degrees_t := REAL8;
EXPORT h3_rads_t := REAL8;
EXPORT h3_resolution_t := UNSIGNED4;
EXPORT h3_latlng_t := SET OF h3_degrees_t;
EXPORT h3_boundary_t := SET OF h3_degrees_t;
```

### Indexing functions

#### index

```c
h3_index_t index(CONST h3_degrees_t lat, CONST h3_degrees_t lng, CONST h3_resolution_t resolution)
```

Indexes the location at the specified `lat`, `lng` and `resolution` (0->15).

Returns 0 on error.

#### center

```c
SET OF h3_degrees_t center(CONST h3_index_t idx)
```

Calculates the centroid of the `idx` and returns the latitude / longitude as a 2 value set.

#### boundary

```c
SET OF h3_degrees_t boundary(CONST h3_index_t idx)
```

Calculates the center and boundary (hexagon) of the `idx` and returns them as  latitude/longitude points (14 value set).

### Inspection functions

#### resolution

```c
h3_resolution_t resolution(CONST h3_index_t idx)
```

Returns the resolution of the `idx`.

#### baseCell

```c
UNSIGNED4 baseCell(CONST h3_index_t idx)
```

Returns the base cell number of the `idx`.

#### toString

```c
STRING toString(CONST h3_index_t idx)
```

Converts the H3Index representation of `idx` to the string representation.

#### fromString

```c
h3_index_t fromString(CONST VARSTRING strIdx) : cpp,action,context,fold,entrypoint='fromString';
```

Converts the string representation of `strIdx` to the H3Index representation.

Returns 0 on error.

#### isValid

```c
BOOLEAN isValid(CONST h3_index_t idx)
```

Returns TRUE if this is a valid **H3** `idx`.

### Grid traversal functions

#### kRing

```c
SET OF h3_index_t kRing(CONST h3_index_t idx, CONST INTEGER4 k)
```

k-rings produces indices within `k` distance of the origin `idx`.

k-ring 0 is defined as the origin index, k-ring 1 is defined as k-ring 0 and
all neighboring indices, and so on.

Output is placed in the returned SET in no particular order.

#### hexRing

```c
SET OF h3_index_t hexRing(CONST h3_index_t idx, CONST UNSIGNED4 k)
```

Produces the hollow hexagonal ring centered at `idx` with distance of `k`.
 
#### h3Distance

```c
SIGNED4 distance(CONST h3_index_t fromIdx, CONST h3_index_t toIdx)
```

Returns the distance in grid cells between `fromIdx` and `toIdx`.

Returns a negative number if finding the distance failed. Finding the distance can fail because the two
indexes are not comparable (different resolutions), too far apart, or are separated by pentagonal
distortion. This is the same set of limitations as the local IJ coordinate space functions.

### Hierarchical grid functions

#### parent

```c
h3_index_t parent(CONST h3_index_t idx, CONST h3_resolution_t resolution)
```

Returns the parent (coarser) index containing `idx` as resolution `resolution`.

#### children

```c
SET OF h3_index_t children(CONST h3_index_t idx, CONST h3_resolution_t resolution)
```

Retruns `children` with the indexes contained by `idx` at resolution `resolution`.

#### compact

```c
SET OF h3_index_t compact(CONST SET OF h3_index_t indexes)
```

Compacts the set `indexes` as best as possible, and returns the compacted set. 

#### uncompact

```c
SET OF h3_index_t uncompact(CONST SET OF h3_index_t indexes, CONST h3_resolution_t resolution)
```

Uncompacts the compacted set of `indexes` to the resolution `resolution` and returns the uncompacted set. 

### Regions
```c
SET OF h3_index_t polyfill(CONST LINKCOUNTED DATASET(h3_boundary_t) boundary, CONST h3_resolution_t resolution)
```

polyfill takes an array of lat/long representing a polygon (open ended) and returns a dataset of hexagons that are contained by the polygon.

### Miscellaneous functions

#### degsToRads

```c
h3_rads_t degsToRads(CONST h3_degrees_t degrees)
```

Converts `degrees` to radians.

#### radsToDegs

```c
h3_degrees_t radsToDegs(CONST h3_rads_t radians)
```

Converts `radians` to degrees.

#### hexAreaKm2

```c
REAL8 hexAreaKm2(CONST h3_resolution_t resolution)
```

Average hexagon area in square kilometers at the given `resolution`.

#### hexAreaM2

```c
REAL8 hexAreaM2(CONST h3_resolution_t resolution)
```

Average hexagon area in square meters at the given `resolution`.

#### numHexagons

```c
UNSIGNED8 numHexagons(CONST h3_resolution_t resolution)
```

Number of unique **H3** indexes at the given `resolution`.

