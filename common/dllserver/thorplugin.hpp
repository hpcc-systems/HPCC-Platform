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
extern DLLSERVER_API void compressResource(MemoryBuffer & compressed, size32_t len, const void *data);

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
