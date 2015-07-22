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

#include "jiface.hpp"
#include "jlib.hpp"
#include "esp.hpp"
#include "componentstatus.ipp"
#include "ws_machine_esp.ipp"

class CESPComponentStatusInfo : public CInterface, implements IESPComponentStatusInfo
{
    StringAttr reporter;
    StringAttr timeCached;
    IArrayOf<IEspComponentStatus> statusList;

    bool addToCache; //this CESPComponentStatusInfo object is created by ws_machine.UpdateComponentStatus
    __int64 expireTimeStamp;
    int componentStatusID; //the worst component status in the system
    int componentTypeID; //the worst component status in the system
    StringAttr componentStatus; //the worst component status in the system
    StringAttr componentType; //the worst component status in the system
    StringAttr endPoint; //the worst component status in the system
    StringAttr timeReportedStr; //the worst component status in the system
    __int64 timeReported; //the worst component status in the system
    Owned<IEspStatusReport> componentStatusReport; //the worst component status in the system

    bool isSameComponent(const char* ep, int componentTypeID, IConstComponentStatus& status);
    void addStatusReport(const char* reporterIn, const char* timeCachedIn, IConstComponentStatus& csIn,
        IEspComponentStatus& csOut, bool firstReport);
    void addComponentStatus(const char* reporterIn, const char* timeCachedIn, IConstComponentStatus& st);
    void appendUnchangedComponentStatus(IEspComponentStatus& statusOld);
    bool cleanExpiredStatusReports(IArrayOf<IConstStatusReport>& reports);

public:
    IMPLEMENT_IINTERFACE;

    CESPComponentStatusInfo(const char* _reporter);

    static void init(IPropertyTree* cfg);

    inline const char* getReporter() { return reporter.get(); };
    inline const char* getTimeCached() { return timeCached.get(); };
    inline int getComponentStatusID() { return componentStatusID; };
    inline const char* getComponentStatus() { return componentStatus.get(); };
    inline const char* getTimeReportedStr() { return timeReportedStr.get(); };
    inline __int64 getTimeReported() { return timeReported; };
    inline const int getComponentTypeID() { return componentTypeID; };
    inline const char* getComponentType() { return componentType.get(); };
    inline const char* getEndPoint() { return endPoint.get(); };
    inline IEspStatusReport* getStatusReport() { return componentStatusReport; };
    inline IArrayOf<IEspComponentStatus>& getComponentStatusList() { return statusList; };

    int queryComponentTypeID(const char *key);
    int queryComponentStatusID(const char *key);
    virtual bool cleanExpiredComponentReports(IESPComponentStatusInfo& statusInfo);
    virtual void mergeComponentStatusInfoFromReports(IESPComponentStatusInfo& statusInfo);
    virtual void setComponentStatus(IArrayOf<IConstComponentStatus>& statusListIn);
    void mergeCachedComponentStatus(IESPComponentStatusInfo& statusInfo);
};

class CComponentStatusFactory : public CInterface, implements IComponentStatusFactory
{
    IArrayOf<IESPComponentStatusInfo> cache; //multiple caches from different reporter
public:
    IMPLEMENT_IINTERFACE;

    CComponentStatusFactory() { };

    virtual ~CComponentStatusFactory()
    {
        cache.kill();
    };

    virtual void init(IPropertyTree* cfg)
    {
        CESPComponentStatusInfo::init(cfg);
    };

    virtual IESPComponentStatusInfo* getComponentStatus();
    virtual void updateComponentStatus(const char* reporter, IArrayOf<IConstComponentStatus>& statusList);
};

extern "C" COMPONENTSTATUS_API IComponentStatusFactory* getComponentStatusFactory();
