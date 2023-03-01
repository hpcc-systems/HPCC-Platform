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
#include "jexcept.hpp"
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

#include <h3/h3api.h>

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

static void throwOnError(H3Error err)
{
    switch (static_cast<H3ErrorCodes>(err))
    {
    case E_SUCCESS:
        return;
    case E_DOMAIN:
        throw MakeStringException(err, "H3:  Argument was outside of acceptable range (when a more specific error code is not available)");
    case E_LATLNG_DOMAIN:
        throw MakeStringException(err, "H3:  Latitude or longitude arguments were outside of acceptable range");
    case E_RES_DOMAIN:
        throw MakeStringException(err, "H3:  Resolution argument was outside of acceptable range");
    case E_CELL_INVALID:
        throw MakeStringException(err, "H3:  `H3Index` cell argument was not valid");
    case E_DIR_EDGE_INVALID:
        throw MakeStringException(err, "H3:  `H3Index` directed edge argument was not valid");
    case E_UNDIR_EDGE_INVALID:
        throw MakeStringException(err, "H3:  `H3Index` undirected edge argument was not valid"); 
    case E_VERTEX_INVALID:
        throw MakeStringException(err, "H3:  `H3Index` vertex argument was not valid");
    case E_PENTAGON:
        throw MakeStringException(err, "H3:  Pentagon distortion was encountered which the algorithm could not handle it");
    case E_DUPLICATE_INPUT:
        throw MakeStringException(err, "H3:  Duplicate input was encountered in the arguments and the algorithm could not handle it");
    case E_NOT_NEIGHBORS:
        throw MakeStringException(err, "H3:  `H3Index` cell arguments were not neighbors");
    case E_RES_MISMATCH:
        throw MakeStringException(err, "H3:  `H3Index` cell arguments had incompatible resolutions");    
    case E_MEMORY_ALLOC:
        throw MakeStringException(err, "H3:  Necessary memory allocation failed");
    case E_MEMORY_BOUNDS:
        throw MakeStringException(err, "H3:  Bounds of provided memory were not large enough");
    case E_OPTION_INVALID:  
        throw MakeStringException(err, "H3:  Mode or flags argument was not valid.");
    case E_FAILED:
    default:
        throw MakeStringException(err, "H3:  The operation failed but a more specific error is not available");
    }
}

namespace h3
{

//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------

//  Indexing Functions ---
ECL_H3_API unsigned __int64 ECL_H3_CALL index(ICodeContext *_ctx, double lat, double lon, uint32_t resolution)
{
    LatLng location;
    location.lat = ::degsToRads(lat);
    location.lng = ::degsToRads(lon);
    H3Index retVal;
    throwOnError(::latLngToCell(&location, resolution, &retVal));
    return retVal;
}

ECL_H3_API void ECL_H3_CALL center(ICodeContext *_ctx, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    LatLng location;
    throwOnError(::cellToLatLng(index, &location));
    __lenResult = 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);
    double *cur = static_cast<double *>(__result);
    *cur++ = ::radsToDegs(location.lat);
    *cur = ::radsToDegs(location.lng);
}

ECL_H3_API void ECL_H3_CALL boundary(ICodeContext *_ctx, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    CellBoundary boundary;
    throwOnError(::cellToBoundary(index, &boundary));
    __lenResult = boundary.numVerts * 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);

    double *cur = static_cast<double *>(__result);
    for (int v = 0; v < boundary.numVerts; v++)
    {
        *cur++ = ::radsToDegs(boundary.verts[v].lat);
        *cur++ = ::radsToDegs(boundary.verts[v].lng);
    }
}

//  Introspection ---
ECL_H3_API uint32_t ECL_H3_CALL resolution(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::getResolution(index);
}

ECL_H3_API uint32_t ECL_H3_CALL baseCell(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::getBaseCellNumber(index);
}

#define H3_INDEX_MINSTRLEN 17
ECL_H3_API void ECL_H3_CALL toString(ICodeContext *_ctx, size32_t &lenVarStr, char *&varStr, unsigned __int64 index)
{
    varStr = static_cast<char *>(rtlMalloc(H3_INDEX_MINSTRLEN));
    throwOnError(::h3ToString(index, varStr, H3_INDEX_MINSTRLEN));
    lenVarStr = strlen(varStr);
}

ECL_H3_API unsigned __int64 ECL_H3_CALL fromString(ICodeContext *_ctx, const char *strIdx)
{
    H3Index retVal;
    throwOnError(::stringToH3(strIdx, &retVal));
    return retVal;
}

ECL_H3_API bool ECL_H3_CALL isValid(ICodeContext *_ctx, unsigned __int64 index)
{
    return ::isValidCell(index) != 0;
}

//  Traversal  ---
ECL_H3_API void ECL_H3_CALL kRing(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, int32_t k)
{
    int64_t maxBuff;
    throwOnError(::maxGridDiskSize(k, &maxBuff));
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    throwOnError(::gridDisk(index, k, buff));
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

ECL_H3_API void ECL_H3_CALL hexRing(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, uint32_t k)
{
    int64_t maxBuff;
    throwOnError(::maxGridDiskSize(k, &maxBuff));
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    throwOnError(::gridRingUnsafe(index, k, buff));
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

ECL_H3_API int32_t ECL_H3_CALL distance(ICodeContext *_ctx, unsigned __int64 indexFrom, unsigned __int64 indexTo)
{
    int64_t distance;
    throwOnError(::gridDistance(indexFrom, indexTo, &distance));
    return distance;
}

//  Hierarchy  ---
ECL_H3_API unsigned __int64 ECL_H3_CALL parent(ICodeContext *_ctx, unsigned __int64 index, uint32_t resolution)
{
    H3Index parent;
    throwOnError(::cellToParent(index, resolution, &parent));
    return parent;
}

ECL_H3_API void ECL_H3_CALL children(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, uint32_t resolution)
{
    int64_t childRes;
    throwOnError(::cellToChildrenSize(index, resolution, &childRes));
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(childRes, sizeof(H3Index)));
    throwOnError(::cellToChildren(index, resolution, buff));
    toSetOf(__isAllResult, __lenResult, __result, buff, childRes);
}

ECL_H3_API void ECL_H3_CALL compact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet)
{
    const int len = lenIndexSet / sizeof(H3Index);
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(len, sizeof(H3Index)));
    throwOnError(::compactCells(static_cast<const H3Index *>(indexSet), buff, len));
    toSetOf(__isAllResult, __lenResult, __result, buff, len);
}

ECL_H3_API void ECL_H3_CALL uncompact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet, uint32_t resolution)
{
    const int len = lenIndexSet / sizeof(H3Index);
    int64_t maxBuff;
    throwOnError(::uncompactCellsSize(static_cast<const H3Index *>(indexSet), len, resolution, &maxBuff));
    H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
    throwOnError(::uncompactCells(static_cast<const H3Index *>(indexSet), len, buff, maxBuff, resolution));
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
}

//  Regions  ---

uint32_t initPolygon(LatLng *verts, size32_t numVerts, uint32_t resolution, GeoPolygon &polygon)
{
    polygon.geoloop.numVerts = numVerts;
    polygon.geoloop.verts = verts;
    polygon.numHoles = 0;
    polygon.holes = NULL;
    int64_t retVal;
    throwOnError(::maxPolygonToCellsSize(&polygon, resolution, 0, &retVal));
    return retVal;
}

ECL_H3_API void ECL_H3_CALL polyfill(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, size32_t countBoundary, const byte **boundary, uint32_t resolution)
{
    //  Check for special case when points exceed 180 degrees (longtitude)
    //   - https://github.com/uber/h3/issues/210
    LatLng *poly = static_cast<LatLng *>(rtlCalloc(countBoundary, sizeof(LatLng)));
    LatLng *wPoly = static_cast<LatLng *>(rtlCalloc(countBoundary, sizeof(LatLng)));
    LatLng *ePoly = static_cast<LatLng *>(rtlCalloc(countBoundary, sizeof(LatLng)));
    double west = 0;
    double east = 0;
    for (int i = 0; i < countBoundary; ++i)
    {
        const LatLng *row = (LatLng *)boundary[i];
        double lat = ::degsToRads(row->lat);
        double lon = ::degsToRads(row->lng);
        poly[i].lat = lat;
        poly[i].lng = lon;
        wPoly[i].lat = lat;
        wPoly[i].lng = lon > 0 ? 0 : lon;
        ePoly[i].lat = lat;
        ePoly[i].lng = lon <= 0 ? 0 : lon;

        if (i == 0)
            west = east = row->lng;
        else if (west > row->lng)
            west = row->lng;
        else if (east < row->lng)
            east = row->lng;
    }
    if (east - west >= 180)
    {
        GeoPolygon wPolygon, ePolygon;
        int wMaxBuff = initPolygon(wPoly, countBoundary, resolution, wPolygon);
        int eMaxBuff = initPolygon(ePoly, countBoundary, resolution, ePolygon);
        H3Index *buff = static_cast<H3Index *>(rtlCalloc(wMaxBuff + eMaxBuff, sizeof(H3Index)));
        throwOnError(::polygonToCells(&wPolygon, resolution, 0, buff));
        throwOnError(::polygonToCells(&ePolygon, resolution, 0, buff + wMaxBuff));
        toSetOf(__isAllResult, __lenResult, __result, buff, wMaxBuff + eMaxBuff);
    }
    else
    {
        GeoPolygon polygon;
        int maxBuff = initPolygon(poly, countBoundary, resolution, polygon);
        H3Index *buff = static_cast<H3Index *>(rtlCalloc(maxBuff, sizeof(H3Index)));
        throwOnError(::polygonToCells(&polygon, resolution, 0, buff));
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
    double retVal;
    throwOnError(::getHexagonAreaAvgKm2(resolution, &retVal));
    return retVal;
}

ECL_H3_API double ECL_H3_CALL hexAreaM2(ICodeContext *_ctx, uint32_t resolution)
{
    double retVal;
    throwOnError(::getHexagonAreaAvgM2(resolution, &retVal));
    return retVal;
}

ECL_H3_API unsigned __int64 ECL_H3_CALL numHexagons(ICodeContext *_ctx, uint32_t resolution)
{
    int64_t retVal;
    throwOnError(::getNumCells(resolution, &retVal));
    return retVal;
}

// ECL Optimized  ---
const unsigned __int64 MASK_CELL_BASE = 0b01111111;
const unsigned __int64 MASK_CELL_RES = 0b00000111;
const unsigned __int64 MASK_RES = 0b00001111;
#define MASK_GET(valToMask, mask, shift) ((valToMask) >> (shift)) & (mask)
#define MASK_SET(valToMask, mask, shift, val) (((valToMask) & ~(static_cast<unsigned __int64>(mask) << (shift))) | (static_cast<unsigned __int64>(val) << (shift)))

ECL_H3_API void ECL_H3_CALL toECLIndex(ICodeContext *_ctx, char *__result, unsigned __int64 index)
{
    for (unsigned int i = 0; i < 16; ++i)
    {
        const unsigned char resVal = MASK_GET(index, i == 0 ? MASK_CELL_BASE : MASK_CELL_RES, (15 - i) * 3);
        __result[i] = ((i == 0 && resVal == MASK_CELL_BASE) || (i > 0 && resVal == MASK_CELL_RES)) ? ' ' : '0' + resVal;
    }
}

ECL_H3_API void ECL_H3_CALL ECLIndex(ICodeContext *_ctx, char *__result, double lat, double lon, uint32_t resolution)
{
    return toECLIndex(_ctx, __result, index(_ctx, lat, lon, resolution));
}

ECL_H3_API unsigned __int64 ECL_H3_CALL fromECLIndex(ICodeContext *_ctx, const char *eclIndex)
{
    unsigned __int64 __result = 646078419604526808; //  lat:0, lng:0, res:15
    unsigned __int64 res = 0;
    for (unsigned int i = 0; i < 16; ++i)
    {
        unsigned char resVal = eclIndex[i];
        if (resVal == ' ')
        {
            resVal = i == 0 ? MASK_CELL_BASE : MASK_CELL_RES;
        }
        else
        {
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
    for (unsigned int i = 1; i < 16; ++i)
    {
        if (eclIndex[i] == ' ')
        {
            return i - 1;
        }
    }
    return 15;
}

ECL_H3_API void ECL_H3_CALL ECLIndexParent(ICodeContext *_ctx, char *__result, const char *eclIndex, uint32_t resolution)
{
    //  This is equivilant to:  eclIndex[1..resolution + 1];
    for (unsigned int i = 0; i < 16; ++i)
    {
        __result[i] = i <= resolution ? eclIndex[i] : ' ';
    }
}

} // namespace h3
