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

#ifndef __HQLPLUGIN_INCL
#define __HQLPLUGIN_INCL

#include "platform.h"

#define PLUGIN_VERSION 2

#define PLUGIN_IMPLICIT_MODULE 1
#define PLUGIN_DLL_MODULE      4
#define PLUGIN_MULTIPLE_VERSIONS      8
#define ZOMBIE_MODULE         32
#define PLUGIN_SAVEMASK       0x0ff

struct ECLPluginDefinitionBlock
{
    unsigned size;              // Size of passed in structure, filled in by caller
    unsigned magicVersion;      // Filled in by plugin - must be PLUGIN_VERSION
    const char *moduleName;
    const char *ECL;
    unsigned flags;
    const char *version;
    const char *description;
};

struct ECLPluginDefinitionBlockEx : public ECLPluginDefinitionBlock 
{
    const char **compatibleVersions;
};

typedef bool (*EclPluginDefinition) (ECLPluginDefinitionBlock *);

//Enable DLL to call host to process memory allocation
struct IPluginContext
{
    virtual void * ctxMalloc(size_t size) =0;
    virtual void * ctxRealloc(void * memblock, size_t size) =0;
    virtual void   ctxFree(void * memblock)=0;
    virtual char * ctxStrdup(char * str)=0;

};
struct IPluginContextEx : public IPluginContext
{
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const = 0;
    virtual const char *ctxQueryProp(const char *propName) const = 0;
};

typedef bool (*EclPluginSetCtx) (IPluginContext *);
typedef bool (*EclPluginSetCtxEx) (IPluginContextEx *);
#define CTXMALLOC(ctx,l)    (ctx ? ctx->ctxMalloc(l) : malloc(l))
#define CTXREALLOC(ctx,p,l) (ctx ? ctx->ctxRealloc(p,l) : realloc(p,l))
#define CTXFREE(ctx,p)      (ctx ? ctx->ctxFree(p) : free(p))
#define CTXSTRDUP(ctx,p)    (ctx ? ctx->ctxStrdup((char*)p) : strdup((char*)p))

inline void * ctxDup(IPluginContext * ctx, const void * data, unsigned len)
{
    void * tgt = CTXMALLOC(ctx, len);
    if (tgt)
        memcpy(tgt, data, len);
    return tgt;
}

#define CTXDUP(ctx,p,l)     (ctxDup(ctx, p, l))

#endif  //__HQLPLUGIN_INCL
