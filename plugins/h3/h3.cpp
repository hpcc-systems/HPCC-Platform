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
    pb->description = "ECL plugin library for uber h3\n";
    return true;
}

#include <h3api.h>

void toSetOf(bool &__isAllResult, size32_t &__lenResult, void *&__result, const H3Index *buff, int maxBuffLen)
{
    int len = 0;
    while (len < maxBuffLen && buff[len] != 0)
        ++len;

    __isAllResult = false;
    __lenResult = len * sizeof(H3Index);
    __result = rtlMalloc(__lenResult);
    memcpy(__result, buff, __lenResult);
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

ECL_H3_API void ECL_H3_CALL center(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    GeoCoord location;
    ::h3ToGeo(index, &location);
    __isAllResult = false;
    __lenResult = 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);
    double *cur = static_cast<double *>(__result);
    *cur = ::radsToDegs(location.lat);
    *(++cur) = ::radsToDegs(location.lon);
}

ECL_H3_API void ECL_H3_CALL boundary(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index)
{
    GeoBoundary boundary;
    ::h3ToGeoBoundary(index, &boundary);
    __isAllResult = false;
    __lenResult = boundary.numVerts * 2 * sizeof(double);
    __result = rtlMalloc(__lenResult);

    double *cur = static_cast<double *>(__result);
    for (int v = 0; v < boundary.numVerts; v++)
    {
        *cur = ::radsToDegs(boundary.verts[v].lat);
        *(++cur) = ::radsToDegs(boundary.verts[v].lon);
        ++cur;
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

ECL_H3_API void ECL_H3_CALL toString(ICodeContext *_ctx, size32_t &lenVarStr, char *&varStr, unsigned __int64 index)
{
    char buff[17];
    ::h3ToString(index, buff, 17);
    lenVarStr = strlen(buff);
    varStr = static_cast<char *>(rtlMalloc(lenVarStr));
    memcpy(varStr, buff, lenVarStr);
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
    H3Index *buff = static_cast<H3Index *>(calloc(maxBuff, sizeof(H3Index)));
    ::kRing(index, k, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
    free(buff);
}

ECL_H3_API void ECL_H3_CALL hexRing(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, unsigned __int64 index, uint32_t k)
{
    int maxBuff = ::maxKringSize(k);
    H3Index *buff = static_cast<H3Index *>(calloc(maxBuff, sizeof(H3Index)));
    ::hexRing(index, k, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
    free(buff);
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
    H3Index *buff = static_cast<H3Index *>(calloc(maxBuff, sizeof(H3Index)));
    ::h3ToChildren(index, resolution, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
    free(buff);
}

ECL_H3_API void ECL_H3_CALL compact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet)
{
    const int len = lenIndexSet / sizeof(H3Index);
    H3Index *buff = static_cast<H3Index *>(calloc(len, sizeof(H3Index)));
    ::compact(static_cast<const H3Index *>(indexSet), buff, len);
    toSetOf(__isAllResult, __lenResult, __result, buff, len);
    free(buff);
}

ECL_H3_API void ECL_H3_CALL uncompact(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, bool isAllIndexSet, size32_t lenIndexSet, const void *indexSet, uint32_t resolution)
{
    const int len = lenIndexSet / sizeof(H3Index);
    int maxBuff = ::maxUncompactSize(static_cast<const H3Index *>(indexSet), len, resolution);
    H3Index *buff = static_cast<H3Index *>(calloc(maxBuff, sizeof(H3Index)));
    ::uncompact(static_cast<const H3Index *>(indexSet), len, buff, maxBuff, resolution);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);
    free(buff);
}

//  Regions  ---
ECL_H3_API void ECL_H3_CALL polyfill(ICodeContext *_ctx, bool &__isAllResult, size32_t &__lenResult, void *&__result, size32_t countBoundary, const byte **boundary, uint32_t resolution)
{
    GeoCoord *verts = static_cast<GeoCoord *>(calloc(countBoundary + 1, sizeof(GeoCoord)));
    for (int i = 0; i < countBoundary; ++i)
    {
        GeoCoord *row = (GeoCoord *)boundary[i];
        verts[i].lat = ::degsToRads(row->lat);
        verts[i].lon = ::degsToRads(row->lon);
        if (i == 0)
            verts[countBoundary] = verts[0];
    }

    Geofence geofence;
    geofence.numVerts = countBoundary + 1;
    geofence.verts = verts;

    GeoPolygon polygon;
    polygon.geofence = geofence;
    polygon.numHoles = 0;

    int maxBuff = ::maxPolyfillSize(&polygon, resolution);
    H3Index *buff = static_cast<H3Index *>(calloc(maxBuff, sizeof(H3Index)));
    ::polyfill(&polygon, resolution, buff);
    toSetOf(__isAllResult, __lenResult, __result, buff, maxBuff);

    free(buff);
    free(verts);
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

} // namespace h3
