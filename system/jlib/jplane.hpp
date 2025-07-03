/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef JPLANE_HPP
#define JPLANE_HPP

#include "jiface.hpp"
#include "jlib.hpp"
#include "jstring.hpp"

//---- Storage plane related functions ----------------------------------------------------

interface IPropertyTree;
interface IPropertyTreeIterator;

extern jlib_decl IPropertyTree * getHostGroup(const char * name, bool required);
extern jlib_decl const IPropertyTree * getStoragePlaneConfig(const char * name, bool required);
extern jlib_decl IPropertyTree * getRemoteStorage(const char * name);
extern jlib_decl IPropertyTreeIterator * getRemoteStoragesIterator();
extern jlib_decl IPropertyTreeIterator * getPlanesIterator(const char * category, const char *name);

//MORE: Should use enum class to avoid potential symbol clashes
enum PlaneAttributeType // remember to update planeAttributeInfo in jfile.cpp
{
    BlockedSequentialIO,
    BlockedRandomIO,
    FileSyncWriteClose,
    ConcurrentWriteSupport,
    WriteSyncMarginMs,
    PlaneAttributeCount
};

extern jlib_decl const char *getPlaneAttributeString(PlaneAttributeType attr);
extern jlib_decl unsigned __int64 getPlaneAttributeValue(const char *planeName, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue);
extern jlib_decl const char *findPlaneFromPath(const char *filePath, StringBuffer &result);
//returns true if plane exists, fills resultValue with defaultValue if attribute is unset
extern jlib_decl bool findPlaneAttrFromPath(const char *filePath, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue, unsigned __int64 &resultValue);
extern jlib_decl size32_t getBlockedFileIOSize(const char *planeName, size32_t defaultSize=0);
extern jlib_decl size32_t getBlockedRandomIOSize(const char *planeName, size32_t defaultSize=0);

//---------------------------------------------------------------------------------------------

enum class AccessMode : unsigned
{

    none            = 0x00000000,
    read            = 0x00000001,
    write           = 0x00000002,
    sequential      = 0x00000004,
    random          = 0x00000008,           // corresponds to "random" reason in alias reasons
    noMount         = 0x01000000,           // corresponds to "api" reason in alias reasons

    readRandom      = read | random,
    readSequential  = read | sequential,
    readNoMount     = read | noMount,
    writeSequential = write | sequential,

    readMeta        = read,                  // read access - may not actually read the contents
    writeMeta       = write,                 // write access - may also be used for delete

//The following are used for mechanical replacement of writeattr to update the function prototypes but not change
//the behaviour but allow all the calls to be revisited later to ensure the correct parameter is used.

    tbdRead          = read,                 // writeattr was false
    tbdWrite         = write,                // writeattr was true
};
BITMASK_ENUM(AccessMode);
inline bool isWrite(AccessMode mode) { return (mode & AccessMode::write) != AccessMode::none; }

extern jlib_decl AccessMode getAccessModeFromString(const char *access); // single access mode


//---------------------------------------------------------------------------------------------

interface IStoragePlane;
interface IStoragePlaneAlias: extends IInterface
{
    virtual AccessMode queryModes() const = 0;
    virtual const char *queryPrefix() const = 0 ;
    virtual bool isAccessible() const = 0;
};

//I'm not sure if this should be used in place of an IGroup, probably as system gradually changes
interface IStorageApiInfo;
interface IStoragePlane: extends IInterface
{
    virtual const char * queryPrefix() const = 0;
    virtual unsigned numDevices() const = 0;
    virtual const std::vector<std::string> &queryHosts() const = 0;
    virtual unsigned numDefaultSprayParts() const = 0 ;
    virtual bool queryDirPerPart() const = 0;
    virtual IStoragePlaneAlias *getAliasMatch(AccessMode desiredModes) const = 0;
    virtual IStorageApiInfo *getStorageApiInfo() const = 0;
    virtual bool isAccessible() const = 0;
};

extern jlib_decl const IStoragePlane * getStoragePlaneByName(const char * name, bool required);
extern jlib_decl const IStoragePlane * getStoragePlaneFromPath(const char *filePath, bool required);
extern jlib_decl const IStoragePlane * getDataStoragePlane(const char * name, bool required);
extern jlib_decl const IStoragePlane * getRemoteStoragePlane(const char * name, bool required);
extern jlib_decl IStoragePlane * createStoragePlane(IPropertyTree *meta);

extern jlib_decl bool getDefaultStoragePlane(StringBuffer &ret);
extern jlib_decl bool getDefaultSpillPlane(StringBuffer &ret);
extern jlib_decl bool getDefaultIndexBuildStoragePlane(StringBuffer &ret);
extern jlib_decl bool getDefaultPersistPlane(StringBuffer &ret);
extern jlib_decl bool getDefaultJobTempPlane(StringBuffer &ret);


extern jlib_decl unsigned getNumPlaneStripes(const char *clusterName);
extern jlib_decl bool isHostInPlane(IPropertyTree *plane, const char *host, bool ipMatch);
extern jlib_decl bool getPlaneHost(StringBuffer &host, const IPropertyTree *plane, unsigned which);
extern jlib_decl void getPlaneHosts(StringArray &hosts, const IPropertyTree *plane);
extern jlib_decl bool isPathInPlane(IPropertyTree *plane, const char *path);

#endif
