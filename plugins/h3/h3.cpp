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

#include "platform.h"
#include "eclrtl.hpp"
#include "h3.hpp"

#define H3_VERSION "h3 plugin 1.0.0"
ECL_H3_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    /*  Warning:    This function may be called without the plugin being loaded fully.
     *              It should not make any library calls or assume that dependent modules
     *              have been loaded or that it has been initialised.
     *
     *              Specifically:  "The system does not call DllMain for process and thread
     *              initialization and termination.  Also, the system does not load
     *              additional executable modules that are referenced by the specified module."
     */

    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = H3_VERSION;
    pb->moduleName = "lib_h3";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for uber h3";
    return true;
}

#include <h3api.h>

static void toSetOf(bool &__isAllResult, size32_t &__lenResult, void *&__result, H3Index *buff, int maxBuffLen)
{
    int finger = 0;
    for (int i = 0; i < maxBuffLen; ++i)
    {
        if (buff[i] == 0)
            continue;

        if (i > finger)
            buff[finger] = buff[i];

        ++finger;
    }
    __isAllResult = false;
    __lenResult = finger * sizeof(H3Index);
    __result = buff;
}

namespace h3
{

//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------

//  Indexing Functions ---
ECL_H3_API unsigned __int64 ECL_H3_CALL index(ICodeContext *_ctx, double lat, double lon, uint32_t resolution)
{
    GeoCoord location;
    location.lat = ::degsToRads(lat);
    location.lon = ::degsToRads(lon);
    return ::geoToH3(&location, resolution);
}

ECL_H3_API void ECL_H3_CALL center(ICodeContext *_ctx, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    GeoCoord location;
    ::h3ToGeo(index, &location);
    __lenResult = 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);
    double *cur = static_cast<double *>(__result);
    *cur++ = ::radsToDegs(location.lat);
    *cur = ::radsToDegs(location.lon);
}

ECL_H3_API void ECL_H3_CALL boundary(ICodeContext *_ctx, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    GeoBoundary boundary;
    ::h3ToGeoBoundary(index, &boundary);
    __lenResult = boundary.numVerts * 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);

    double *cur = static_cast<double *>(__result);
    for (int v = 0; v < boundary.numVerts; v++)
    {
        *cur++ = ::radsToDegs(boundary.verts[v].lat);
        *cur++ = ::radsToDegs(boundary.verts[v].lon);
    }
}

//  Introspection ---
ECL_H3_API uint32_t ECL_H3_CALL resolution(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::h3GetResolution(index);
}

ECL_H3_API uint32_t ECL_H3_CALL baseCell(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::h3GetBaseCell(index);
}

#define H3_INDEX_MINSTRLEN 17
ECL_H3_API void ECL_H3_CALL toString(ICodeContext *_ctx, size32_t &lenVarStr, char *&varStr, unsigned __int64 index)
{
    varStr = static_cast<char *>(rtlMalloc(H3_INDEX_MINSTRLEN));
    ::h3ToString(index, varStr, H3_INDEX_MINSTRLEN);
    lenVarStr = strlen(varStr);
}

ECL_H3_API unsigned __int64 ECL_H3_CALL fromString(ICodeContext *_ctx, const char *strIdx)
{
    return ::stringToH3(strIdx);
}

ECL_H3_API bool ECL_H3_CALL isValid(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::h3IsValid(index) != 0;
}

//  Traversal  ---
ECL_H3_API void ECL_H3_CALL kRing(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, int32_t k)
{
    int maxBuff = ::maxKringSize(k);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    ::kRing(index, k, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

ECL_H3_API void ECL_H3_CALL hexRing(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, uint32_t k)
{
    int maxBuff = ::maxKringSize(k);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    ::hexRing(index, k, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

ECL_H3_API int32_t ECL_H3_CALL distance(ICodeContext *_ctx, unsigned __int64 indexFrom, unsigned __int64 indexTo)
{
    return ::h3Distance(indexFrom, indexTo);
}

//  Hierarchy  ---
ECL_H3_API unsigned __int64 ECL_H3_CALL parent(ICodeContext *_ctx, unsigned __int64 index, uint32_t resolution)
{
    return ::h3ToParent(index, resolution);
}

ECL_H3_API void ECL_H3_CALL children(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, uint32_t resolution)
{
    int maxBuff = ::maxH3ToChildrenSize(index, resolution);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    ::h3ToChildren(index, resolution, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

ECL_H3_API void ECL_H3_CALL compact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet)
{
    const int len = lenIndexSet / sizeof(H3Index);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(len, sizeof(H3Index)));
    ::compact(static_cast<const H3Index *>(indexSet), buff, len);
    toSetOf(__isAllResult, __lenResult, __result, buff, len);
}

ECL_H3_API void ECL_H3_CALL uncompact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet, uint32_t resolution)
{
    const int len = lenIndexSet / sizeof(H3Index);
    int maxBuff = ::maxUncompactSize(static_cast<const H3Index *>(indexSet), len, resolution);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    ::uncompact(static_cast<const H3Index *>(indexSet), len, buff, maxBuff, resolution);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

//  Regions  ---

uint32_t initPolygon(GeoCoord *verts, size32_t numVerts, uint32_t resolution, GeoPolygon &polygon)
{
    polygon.geofence.numVerts = numVerts;
    polygon.geofence.verts = verts;
    polygon.numHoles = 0;
    polygon.holes = NULL;
    return ::maxPolyfillSize(&polygon, resolution);
}

ECL_H3_API void ECL_H3_CALL polyfill(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, size32_t countBoundary, const byte **boundary, uint32_t resolution)
{
    //  Check for special case when points exceed 180 degrees (longtitude)
    //   - https://github.com/uber/h3/issues/210
    GeoCoord *poly = static_cast<GeoCoord *>(rtlCalloc(countBoundary, sizeof(GeoCoord)));
    GeoCoord *wPoly = static_cast<GeoCoord *>(rtlCalloc(countBoundary, sizeof(GeoCoord)));
    GeoCoord *ePoly = static_cast<GeoCoord *>(rtlCalloc(countBoundary, sizeof(GeoCoord)));
    double west = 0;
    double east = 0;
    for (int i = 0; i < countBoundary; ++i)
    {
        const GeoCoord *row = (GeoCoord *)boundary[i];
        double lat = ::degsToRads(row->lat);
        double lon = ::degsToRads(row->lon);
        poly[i].lat = lat;
        poly[i].lon = lon;
        wPoly[i].lat = lat;
        wPoly[i].lon = lon > 0 ? 0 : lon;
        ePoly[i].lat = lat;
        ePoly[i].lon = lon <= 0 ? 0 : lon;

        if (i == 0)
            west = east = row->lon;
        else if (west > row->lon)
            west = row->lon;
        else if (east < row->lon)
            east = row->lon;
    }
    if (east - west >= 180)
    {
        GeoPolygon wPolygon, ePolygon;
        int wMaxBuff = initPolygon(wPoly, countBoundary, resolution, wPolygon);
        int eMaxBuff = initPolygon(ePoly, countBoundary, resolution, ePolygon);
        H3Index *buff = static_cast<H3Index *>(rtlCalloc(wMaxBuff + eMaxBuff, sizeof(H3Index)));
        ::polyfill(&wPolygon, resolution, buff);
        ::polyfill(&ePolygon, resolution, buff + wMaxBuff);
        toSetOf(__isAllResult, __lenResult, __result, buff, wMaxBuff + eMaxBuff);
    }
    else
    {
        GeoPolygon polygon;
        int maxBuff = initPolygon(poly, countBoundary, resolution, polygon);
        H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
        ::polyfill(&polygon, resolution, buff);
        toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
    }
    rtlFree(ePoly);
    rtlFree(wPoly);
    rtlFree(poly);
}

//  Misc  ---
ECL_H3_API double ECL_H3_CALL degsToRads(ICodeContext *_ctx, double degrees)
{
    return ::degsToRads(degrees);
}

ECL_H3_API double ECL_H3_CALL radsToDegs(ICodeContext *_ctx, double rads)
{
    return ::radsToDegs(rads);
}

ECL_H3_API double ECL_H3_CALL hexAreaKm2(ICodeContext *_ctx, uint32_t resolution)
{
    return ::hexAreaKm2(resolution);
}

ECL_H3_API double ECL_H3_CALL hexAreaM2(ICodeContext *_ctx, uint32_t resolution)
{
    return ::hexAreaM2(resolution);
}

ECL_H3_API unsigned __int64 ECL_H3_CALL numHexagons(ICodeContext *_ctx, uint32_t resolution)
{
    return ::numHexagons(resolution);
}

// ECL Optimized  ---
const unsigned __int64 MASK_CELL_BASE = 0b01111111;
const unsigned __int64 MASK_CELL_RES = 0b00000111;
const unsigned __int64 MASK_RES = 0b00001111;
#define MASK_GET(valToMask, mask, shift) ((valToMask) >> (shift)) & (mask)
#define MASK_SET(valToMask, mask, shift, val) (((valToMask) & ~(static_cast<unsigned __int64>(mask) << (shift))) | (static_cast<unsigned __int64>(val) << (shift)))

ECL_H3_API void ECL_H3_CALL toECLIndex(ICodeContext *_ctx, char * __result, unsigned __int64 index)
{
    for (unsigned int i = 0; i < 16; ++i) {
        const unsigned char resVal = MASK_GET(index, i == 0 ? MASK_CELL_BASE : MASK_CELL_RES, (15 - i) * 3);
        __result[i] = ((i == 0 && resVal == MASK_CELL_BASE) || (i > 0 && resVal == MASK_CELL_RES)) ? ' ' : '0' + resVal;
    }
}

ECL_H3_API void ECL_H3_CALL ECLIndex(ICodeContext *_ctx, char * __result, double lat, double lon, uint32_t resolution)
{
    return toECLIndex(_ctx, __result, index(_ctx, lat, lon, resolution));
}

ECL_H3_API unsigned __int64 ECL_H3_CALL fromECLIndex(ICodeContext *_ctx, const char *eclIndex)
{
    unsigned __int64 __result = 646078419604526808; //  lat:0, lng:0, res:15
    unsigned __int64 res = 0;
    for (unsigned int i = 0; i < 16; ++i) {
        unsigned char resVal = eclIndex[i];
        if (resVal == ' ') {
            resVal = i == 0 ? MASK_CELL_BASE : MASK_CELL_RES;
        } else {
            resVal -= '0';
            res = i;
        }
        __result = MASK_SET(__result, i == 0 ? MASK_CELL_BASE : MASK_CELL_RES, (15 - i) * 3, resVal);
    }
    return MASK_SET(__result, MASK_RES, 52, res);
}

ECL_H3_API uint32_t ECL_H3_CALL ECLIndexResolution(ICodeContext *_ctx, const char *eclIndex)
{
    //  This is equivilant to:  LENGTH(TRIM(eclIndex)) - 1;
    for (unsigned int i = 1; i < 16; ++i) {
        if (eclIndex[i] == ' ') {
            return i - 1;
        }
    }
    return 15;
}

ECL_H3_API void ECL_H3_CALL ECLIndexParent(ICodeContext *_ctx, char * __result, const char *eclIndex, uint32_t resolution)
{
    //  This is equivilant to:  eclIndex[1..resolution + 1];
    for (unsigned int i = 0; i < 16; ++i) {
        __result[i] = i <= resolution ? eclIndex[i] : ' ';
    }
}

} // namespace h3
