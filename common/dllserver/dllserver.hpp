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

#ifndef DLLSERVER_INCL
#define DLLSERVER_INCL

#ifdef _WIN32
    #ifdef DLLSERVER_EXPORTS
        #define DLLSERVER_API __declspec(dllexport)
    #else
        #define DLLSERVER_API __declspec(dllimport)
    #endif
#else
    #define DLLSERVER_API
#endif

#include "jiface.hpp"

class IpAddress;
class RemoteFilename;
class MemoryBuffer;
class StringBuffer;
interface IJlibDateTime;

enum DllLocationType
{
    DllLocationNowhere = 0,
    DllLocationAnywhere = 1,
    DllLocationDomain = 2,
    DllLocationLocal = 3,
    DllLocationDirectory = 4,
    DllLocationSize = 5
};

interface IDllLocation : extends IInterface
{
    virtual void getDllFilename(RemoteFilename & filename) = 0;
    virtual bool getLibFilename(RemoteFilename & filename) = 0;
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
    virtual void getLibrary(const char * name, MemoryBuffer & dllText) = 0;
    virtual void getLocalLibraryName(const char * name, StringBuffer & libraryName) = 0;
    virtual DllLocationType isAvailable(const char * name) = 0;
    virtual ILoadedDllEntry * loadDll(const char * name, DllLocationType location) = 0;
    virtual void removeDll(const char * name, bool removeDlls, bool removeDirectory) = 0;
    virtual void registerDll(const char * name, const char * kind, const char * dllPath) = 0;
};

extern "C" DLLSERVER_API IDllServer & queryDllServer();
extern "C" DLLSERVER_API void closeDllServer();
extern "C" DLLSERVER_API void initDllServer(const char * localRoot);
extern DLLSERVER_API void testDllServer();

#endif
