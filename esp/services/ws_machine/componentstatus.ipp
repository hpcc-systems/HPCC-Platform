/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#pragma warning (disable : 4786)

#ifndef _COMPONENTSTATUS_HPP__
#define _COMPONENTSTATUS_HPP__

#include "jiface.hpp"

class StringBuffer;
class IPropertyTree;
class IPropertyTreeIterator;

class IEspStatusReport;
class IEspComponentStatus;
class IConstComponentStatus;
template<class IEspComponentStatus> class IArrayOf;


#ifdef WIN32
    #ifdef SMCLIB_EXPORTS
        #define COMPONENTSTATUS_API __declspec(dllexport)
    #else
        #define COMPONENTSTATUS_API __declspec(dllimport)
    #endif
#else
    #define COMPONENTSTATUS_API
#endif

class IESPComponentStatusInfo : public IInterface
{
public:
    virtual int queryComponentTypeID(const char *key) = 0;
    virtual int queryComponentStatusID(const char *key) = 0;

    virtual const char* getReporter() = 0;
    virtual const char* getTimeCached() = 0;
    virtual const char* getComponentStatus() = 0;
    virtual int getComponentStatusID() = 0;
    virtual const char* getTimeReportedStr() = 0;
    virtual __int64 getTimeReported() = 0;
    virtual const char* getEndPoint() = 0;
    virtual const char* getComponentType() = 0;
    virtual const int getComponentTypeID() = 0;
    virtual IEspStatusReport* getStatusReport() = 0;
    virtual IArrayOf<IEspComponentStatus>& getComponentStatusList() = 0;
    virtual void setComponentStatus(IArrayOf<IConstComponentStatus>& StatusList) = 0;
    virtual void mergeCachedComponentStatus(IESPComponentStatusInfo& statusInfo) = 0;
    virtual void mergeComponentStatusInfoFromReports(IESPComponentStatusInfo& statusInfo) = 0;
    virtual bool cleanExpiredComponentReports(IESPComponentStatusInfo& statusInfo) = 0;
};

class IComponentStatusFactory : public IInterface
{
public:
    virtual void init(IPropertyTree* cfg) = 0;
    virtual IESPComponentStatusInfo* getComponentStatus() = 0;
    virtual void updateComponentStatus(const char* reporter, IArrayOf<IConstComponentStatus>& StatusList) = 0;
};

#endif  //_COMPONENTSTATUS_HPP__
