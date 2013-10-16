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

#ifndef THORPLUGIN_HPP
#define THORPLUGIN_HPP

#include "hqlplugins.hpp"
#include "dllserver.hpp"

// call release to unload it.
interface ILoadedDllEntry : extends IInterface
{
    virtual void * getEntry(const char * name) const = 0;
    virtual HINSTANCE getInstance() const = 0;
    virtual bool IsShared() = 0;
    virtual const char * queryVersion() const = 0;
    virtual const char * queryName() const = 0;
    virtual const byte * getResource(unsigned id) const = 0;
    virtual bool getResource(size32_t & len, const void * & data, const char * type, unsigned id) const = 0;
};

extern DLLSERVER_API ILoadedDllEntry * createDllEntry(const char *name, bool isGlobal, const IFileIO *dllFile);
extern DLLSERVER_API ILoadedDllEntry * createExeDllEntry(const char *name);
extern DLLSERVER_API bool getEmbeddedWorkUnitXML(ILoadedDllEntry *dll, StringBuffer &xml);
extern DLLSERVER_API bool getEmbeddedManifestXML(ILoadedDllEntry *dll, StringBuffer &xml);
extern DLLSERVER_API bool checkEmbeddedWorkUnitXML(ILoadedDllEntry *dll);
extern DLLSERVER_API bool getResourceXMLFromFile(const char *filename, const char *type, unsigned id, StringBuffer &xml);
extern DLLSERVER_API bool getWorkunitXMLFromFile(const char *filename, StringBuffer &xml);
extern DLLSERVER_API bool getManifestXMLFromFile(const char *filename, StringBuffer &xml);

extern DLLSERVER_API bool decompressResource(size32_t len, const void *data, StringBuffer &result);
extern DLLSERVER_API bool decompressResource(size32_t len, const void *data, MemoryBuffer &result);
extern DLLSERVER_API void compressResource(MemoryBuffer & compressed, size32_t len, const void *data);
extern DLLSERVER_API void appendResource(MemoryBuffer & mb, size32_t len, const void *data, bool compress);

class DLLSERVER_API SimplePluginCtx : implements IPluginContextEx
{
public:
    virtual void * ctxMalloc(size_t size);
    virtual void * ctxRealloc(void * _ptr, size_t size);
    virtual void   ctxFree(void * _ptr);
    virtual char * ctxStrdup(char * _ptr);
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const;
    virtual const char *ctxQueryProp(const char *propName) const;
};

class DLLSERVER_API SafePluginMap : public CInterface
{
    IPluginContextEx * pluginCtx;
    MapStringToMyClass<ILoadedDllEntry> map;
    CriticalSection crit;
    bool trace;
public:
    SafePluginMap(IPluginContextEx * _pluginCtx, bool _trace) 
    : pluginCtx(_pluginCtx), map(true), trace(_trace)
    {
        assertex(pluginCtx);
    }

    ILoadedDllEntry *getPluginDll(const char *id, const char *version, bool checkVersion);
    bool addPlugin(const char *path, const char *dllname);
    void loadFromList(const char * pluginsList);
    void loadFromDirectory(const char * pluginDirectory);
};

#endif // THORPLUGIN_HPP
