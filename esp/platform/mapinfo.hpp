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

#ifndef __MAPINFO_HPP__
#define __MAPINFO_HPP__

#include "jiface.hpp"

interface IMapInfo : public IInterface
{
    virtual double getMinVersion(const char* structName, const char* field) = 0;
    virtual double getMaxVersion(const char* structName, const char* field) = 0;
    virtual double getDeprVersion(const char* structName, const char* field) = 0;
    // support only one optional now
    virtual const char* getOptional(const char* structName, const char* field) = 0;

    virtual void addMaxVersion(const char* structName, const char* field, double version) = 0;
    virtual void addMinVersion(const char* structName, const char* field, double version) = 0;
    virtual void addDeprVersion(const char* structName, const char* field, double version) = 0;
    virtual void addOptional(const char* structName, const char* field, const char* option) = 0;

    // for debug
    virtual void toString(class StringBuffer& s) = 0;
};

IMapInfo* createMapInfo();
IMapInfo* createNullMapInfo();

#endif

