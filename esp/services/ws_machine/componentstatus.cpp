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
#include "componentstatus.hpp"
#include "jlib.hpp"
#include "esp.hpp"
#include "ws_machine_esp.ipp"
#include "ws_machine.hpp"

static unsigned csCacheMinutes = 30; //30 minutes as default
static MapStringTo<int> componentTypeMap;
static MapStringTo<int> componentStatusTypeMap;
static unsigned componentTypeIDCount;

const char* formatTimeStamp(time_t tNow, StringAttr& out)
{
    char timeStr[32];
#ifdef _WIN32
    struct tm *ltNow;
    ltNow = localtime(&tNow);
    strftime(timeStr, 32, "%Y-%m-%d %H:%M:%S", ltNow);
#else
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    strftime(timeStr, 32, "%Y-%m-%d %H:%M:%S", &ltNow);
#endif
    out.set(timeStr);
    return out.get();
}

CESPComponentStatusInfo::CESPComponentStatusInfo(const char* _reporter)
{
    componentStatusID = -1;
    addToCache = _reporter? true : false;
    if (_reporter && *_reporter)
        reporter.set(_reporter);

    expireTimeStamp = 0;
    if (csCacheMinutes > 0)
    { //A status report should be expired if it was reported >csCacheMinutes ago.
        CDateTime timeNow;
        timeNow.setNow();
        timeNow.adjustTime(-csCacheMinutes);
        expireTimeStamp = timeNow.getSimple();
    }
}

void CESPComponentStatusInfo::init(IPropertyTree* cfg)
{
    StringArray statusTypeMap;
    Owned<IPropertyTreeIterator> statusTypes = cfg->getElements("StatusType");
    ForEach(*statusTypes)
    {
        IPropertyTree& statusType = statusTypes->query();
        const char* name = statusType.queryProp("@name");
        if (name && *name)
            componentStatusTypeMap.setValue(name, statusType.getPropInt("@id"));
    }

    if (componentStatusTypeMap.count() < 1)
    {
        componentStatusTypeMap.setValue("normal", 1);
        componentStatusTypeMap.setValue("warning", 2);
        componentStatusTypeMap.setValue("error", 3);
    }
    componentTypeIDCount = 0;

    cfg->getPropInt("CSCacheMinutes", csCacheMinutes);
}

bool CESPComponentStatusInfo::isSameComponent(const char* ep, int componentTypeID, IConstComponentStatus& status)
{
    const char* ep1 = status.getEndPoint();
    if (!ep1 || !*ep1 || !ep || !*ep)
        return false;
    bool hasPort = strchr(ep, ':');
    if (hasPort)
        return streq(ep1, ep);
    //If no port, for now, only one componentType is reported per IP.
    return ((componentTypeID == status.getComponentTypeID()) && streq(ep1, ep));
}

void CESPComponentStatusInfo::addStatusReport(const char* reporterIn, const char* timeCachedIn, IConstComponentStatus& csIn, IEspComponentStatus& csOut, bool firstReport)
{
    IArrayOf<IConstStatusReport>& statusReports = csOut.getStatusReports();
    IArrayOf<IConstStatusReport>& reportsIn = csIn.getStatusReports();
    ForEachItemIn(i, reportsIn)
    {
        IConstStatusReport& report = reportsIn.item(i);
        const char* status = report.getStatus();
        if (!status || !*status)
            continue;

        if ((expireTimeStamp > 0) && (report.getTimeReported() < expireTimeStamp))
            continue;

        int statusID;
        if (addToCache) //from the update status request
            statusID = queryComponentStatusID(status);
        else //from a cache report which StatusID has been set.
            statusID = report.getStatusID();

        Owned<IEspStatusReport> statusReport = createStatusReport();
        statusReport->setStatusID(statusID);
        statusReport->setStatus(status);

        const char* details = report.getStatusDetails();
        if (details && *details)
            statusReport->setStatusDetails(details);

        const char* url = report.getURL();
        if (url && *url)
            statusReport->setURL(url);

        statusReport->setReporter(reporterIn);
        statusReport->setTimeCached(timeCachedIn);
        statusReport->setTimeReported(report.getTimeReported());

        if (!addToCache)
        {//We need to add more info for a user-friendly output
            StringAttr timeStr;
            statusReport->setTimeReportedStr(formatTimeStamp(report.getTimeReported(), timeStr));

            if (firstReport || (statusID > csOut.getStatusID())) //worst case for component
            {
                csOut.setStatusID(statusID);
                csOut.setStatus(status);
                csOut.setTimeReportedStr(timeStr.get());
                csOut.setReporter(reporterIn);
            }
            if (statusID > componentStatusID) //worst case for whole system
            {
                componentStatusID = statusID;
                componentStatus.set(status);
                timeReported = report.getTimeReported();
                timeReportedStr.set(timeStr.get());
                reporter.set(reporterIn);
                componentTypeID = csIn.getComponentTypeID();
                componentType.set(csIn.getComponentType());
                endPoint.set(csIn.getEndPoint());
                componentStatusReport.setown(statusReport.getLink());
            }
        }
        statusReports.append(*statusReport.getClear());
    }
}

void CESPComponentStatusInfo::addComponentStatus(const char* reporterIn, const char* timeCachedIn, IConstComponentStatus& st)
{
    Owned<IEspComponentStatus> cs = createComponentStatus();
    cs->setEndPoint(st.getEndPoint());
    cs->setComponentType(st.getComponentType());

    int componentTypeID;
    if (addToCache) //from the update status request
        componentTypeID = queryComponentTypeID(st.getComponentType());
    else
        componentTypeID = st.getComponentTypeID();
    cs->setComponentTypeID(componentTypeID);

    IArrayOf<IConstStatusReport> statusReports;
    cs->setStatusReports(statusReports);
    addStatusReport(reporterIn, timeCachedIn, st, *cs, true);

    statusList.append(*cs.getClear());
}

void CESPComponentStatusInfo::appendUnchangedComponentStatus(IEspComponentStatus& statusOld)
{
    bool componentFound = false;
    const char* ep = statusOld.getEndPoint();
    int componentTypeID = statusOld.getComponentTypeID();
    ForEachItemIn(i, statusList)
    {
        if (isSameComponent(ep, componentTypeID, statusList.item(i)))
        {
            componentFound =  true;
            break;
        }
    }
    if (!componentFound)
        addComponentStatus(reporter.get(), timeCached, statusOld);
}

bool CESPComponentStatusInfo::cleanExpiredStatusReports(IArrayOf<IConstStatusReport>& reports)
{
    bool expired = true;
    ForEachItemInRev(i, reports)
    {
        IConstStatusReport& report = reports.item(i);
        if ((expireTimeStamp > 0) && (report.getTimeReported() < expireTimeStamp))
            reports.remove(i);
        else
            expired = false;
    }
    return expired;
}

int CESPComponentStatusInfo::queryComponentTypeID(const char *key)
{
    int* id = componentTypeMap.getValue(key);
    if (id)
        return *id;

    componentTypeMap.setValue(key, ++componentTypeIDCount);
    return componentTypeIDCount;
}

int CESPComponentStatusInfo::queryComponentStatusID(const char *key)
{
    StringBuffer buf(key);
    int* value = componentStatusTypeMap.getValue(buf.toLowerCase().str());
    if (!value)
        return 0;
    return *value;
}

bool CESPComponentStatusInfo::cleanExpiredComponentReports(IESPComponentStatusInfo& statusInfo)
{
    bool expired = true;
    IArrayOf<IEspComponentStatus>& statusList = statusInfo.getComponentStatusList();
    ForEachItemInRev(i, statusList)
    {
        IEspComponentStatus& status = statusList.item(i);
        if (cleanExpiredStatusReports(status.getStatusReports()))
            statusList.remove(i);
        else
            expired = false;
    }
    return expired;
}

void CESPComponentStatusInfo::mergeComponentStatusInfoFromReports(IESPComponentStatusInfo& statusInfo)
{
    const char* reporterIn = statusInfo.getReporter();
    const char* timeCachedIn = statusInfo.getTimeCached();
    IArrayOf<IEspComponentStatus>& statusListIn = statusInfo.getComponentStatusList();
    ForEachItemIn(i, statusListIn)
    {
        IEspComponentStatus& statusIn = statusListIn.item(i);

        bool newCompoment = true;
        const char* ep = statusIn.getEndPoint();
        int componentTypeID = statusIn.getComponentTypeID();
        ForEachItemIn(ii, statusList)
        {
            IEspComponentStatus& statusOut = statusList.item(ii);
            if (isSameComponent(ep, componentTypeID, statusOut))
            {// multiple reports from different reporters.
                addStatusReport(reporterIn, timeCachedIn, statusIn, statusOut, false);
                newCompoment =  false;
                break;
            }
        }
        if (newCompoment)
            addComponentStatus(reporterIn, timeCachedIn, statusIn);
    }
}

void CESPComponentStatusInfo::setComponentStatus(IArrayOf<IConstComponentStatus>& statusListIn)
{
    time_t tNow;
    time(&tNow);
    formatTimeStamp(tNow, timeCached);

    statusList.kill();
    ForEachItemIn(i, statusListIn)
        addComponentStatus(reporter, timeCached, statusListIn.item(i));
}

void CESPComponentStatusInfo::mergeCachedComponentStatus(IESPComponentStatusInfo& statusInfo)
{
    IArrayOf<IEspComponentStatus>& csList = statusInfo.getComponentStatusList();
    ForEachItemIn(i, csList)
        appendUnchangedComponentStatus(csList.item(i));
}

static CriticalSection componentStatusSect;

IESPComponentStatusInfo* CComponentStatusFactory::getComponentStatus()
{
    CriticalBlock block(componentStatusSect);
    Owned<IESPComponentStatusInfo> status = new CESPComponentStatusInfo(NULL);
    ForEachItemInRev(i, cache)
    {
        IESPComponentStatusInfo& statusInfo = cache.item(i);
        if (status->cleanExpiredComponentReports(statusInfo))
            cache.remove(i);
        else
            status->mergeComponentStatusInfoFromReports(statusInfo);
    }
    return status.getClear();
}

void CComponentStatusFactory::updateComponentStatus(const char* reporter, IArrayOf<IConstComponentStatus>& statusList)
{
    CriticalBlock block(componentStatusSect);

    Owned<IESPComponentStatusInfo> status = new CESPComponentStatusInfo(reporter);
    status->setComponentStatus(statusList);

    ForEachItemIn(i, cache)
    {
        IESPComponentStatusInfo& cachedStatus = cache.item(i);
        if (strieq(reporter, cachedStatus.getReporter()))
        {
            status->mergeCachedComponentStatus(cachedStatus);
            cache.remove(i);
            break;
        }
    }
    cache.append(*status.getClear());
}

static CComponentStatusFactory *csFactory = NULL;

static CriticalSection getComponentStatusSect;

extern COMPONENTSTATUS_API IComponentStatusFactory* getComponentStatusFactory()
{
    CriticalBlock block(getComponentStatusSect);

    if (!csFactory)
        csFactory = new CComponentStatusFactory();

    return LINK(csFactory);
}
