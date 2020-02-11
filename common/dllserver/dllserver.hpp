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

#ifndef DLLSERVER_INCL
#define DLLSERVER_INCL

#ifdef DLLSERVER_EXPORTS
    #define DLLSERVER_API DECL_EXPORT
#else
    #define DLLSERVER_API DECL_IMPORT
#endif

#include "jiface.hpp"

class IpAddress;
class RemoteFilename;
class MemoryBuffer;
class StringBuffer;
interface IJlibDateTime;
/**
 * IDLLLocation tracks where a generated DLL (or other artefact?) is located, and supports copying them to a location where they are required
 * In general this means copying from a location where they are only accessible via dafilesrv (and cannot therefore be directly loaded) to
 * a local location where they can. However, DLLs in shared off-node mounts are not currently describable.
 */
enum DllLocationType
{
    DllLocationNowhere = 0,       // DLL does not exist anywhere
    DllLocationAnywhere = 1,      // DLL exists but cannot be loaded directly
    DllLocationLocal = 2,         // DLL exists on local machine but not in this process's DLL cache directory
    DllLocationDirectory = 3      // DLL exists in this process's cache directory
};

interface IDllLocation : extends IInterface
{
    virtual void getDllFilename(RemoteFilename & filename) = 0;
    virtual void getIP(IpAddress & ip) = 0;
    virtual DllLocationType queryLocation() = 0;
    virtual void remove(bool removeFiles, bool removeDirectory) = 0;
};



interface IDllEntry : extends IInterface
{
    virtual IIterator * createLocationIterator() = 0;
    virtual IDllLocation * getBestLocation() = 0;
    virtual IDllLocation * getBestLocationCandidate() = 0;
    virtual void getCreated(IJlibDateTime & dateTime) = 0;
    virtual const char * queryKind() = 0;
    virtual const char * queryName() = 0;
    virtual void remove(bool removeFiles, bool removeDirectory) = 0;
};


interface ILoadedDllEntry;

interface IDllServer : extends IInterface
{
    virtual IIterator * createDllIterator() = 0;
    virtual void ensureAvailable(const char * name, DllLocationType location) = 0;
    virtual void getDll(const char * name, MemoryBuffer & dllText) = 0;
    virtual IDllEntry * getEntry(const char * name) = 0;
    virtual DllLocationType isAvailable(const char * name) = 0;
    virtual ILoadedDllEntry * loadDll(const char * name, DllLocationType location) = 0;
    virtual ILoadedDllEntry * loadDllResources(const char * name, DllLocationType location) = 0;
    virtual void removeDll(const char * name, bool removeDlls, bool removeDirectory) = 0;
    virtual void registerDll(const char * name, const char * kind, const char * dllPath) = 0;
    virtual IDllEntry * createEntry(IPropertyTree *owner, IPropertyTree *entry) = 0;
};

extern DLLSERVER_API IDllServer & queryDllServer();
extern DLLSERVER_API void closeDllServer();
extern DLLSERVER_API void initDllServer(const char * localRoot);

#endif
