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

#ifndef __HQLPLUGIN_INCL
#define __HQLPLUGIN_INCL

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
