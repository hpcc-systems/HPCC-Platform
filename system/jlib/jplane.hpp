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
#include "jstring.hpp"

//---- Storage plane related functions ----------------------------------------------------

interface IPropertyTree;
interface IPropertyTreeIterator;

extern jlib_decl IPropertyTree * getHostGroup(const char * name, bool required);
extern jlib_decl IPropertyTree * getStoragePlane(const char * name);
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

#endif
