/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef __THOR__
#define __THOR__

#define THOR_VERSION_MAJOR 4
#define THOR_VERSION_MINOR 1

typedef unsigned activity_id;
typedef unsigned graph_id;
#define ACTPF
#define GIDPF

typedef unsigned __int64 rowcount_t;
#define RCPF I64F
#define RCMAX ((rowcount_t)(__int64)-1)
#define RCUNBOUND RCMAX
#define RCUNSET RCMAX
typedef size32_t rowidx_t;
#define RCIDXMAX ((rowidx_t)(size32_t)-1)
#define RIPF ""
#define RIMAX ((rowidx_t)-1)

#include "jexcept.hpp"

// validate that type T doesn't truncate
template <class T>
inline rowcount_t validRC(T X)
{
    if (X != (rowcount_t)X)
        throw MakeStringException(0, "rowcount_t value truncation");
    return (rowcount_t)X;
}

template <class T>
inline rowidx_t validRIDX(T X)
{
    if (X != (rowidx_t)X)
        throw MakeStringException(0, "rowidx_t value truncation");
    return (rowidx_t)X;
}

#if 0
    #define CATCHALL ...
#else
    struct DummyCatchAll{ int i; };
    #define CATCHALL DummyCatchAll
#endif


#endif
