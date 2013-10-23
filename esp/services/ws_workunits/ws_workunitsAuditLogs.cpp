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

#include "ws_workunitsService.hpp"
#include "jlib.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "daaudit.hpp"
#include "exception_util.hpp"
#include "wujobq.hpp"
#include "eventqueue.hpp"
#include "fileview.hpp"
#include "hqlerror.hpp"
#include "sacmd.hpp"
#include "wuwebview.hpp"
#include "portlist.h"
#include "dllserver.hpp"
#include "schedulectrl.hpp"
#include "scheduleread.hpp"

bool getClusterJobQueueXLS(StringBuffer &xml, const char* cluster, const char* startDate, const char* endDate, const char* showType)
{
    CDateTime fromTime;
    if(notEmpty(startDate))
        fromTime.setString(startDate, NULL, false);
    CDateTime toTime;
    if(notEmpty(endDate))
        toTime.setString(endDate, NULL, false);

    xml.append("<XmlSchema name=\"MySchema\">"
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");
    xml.append("<xs:element name=\"datetime\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"running\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"queued\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"connected\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"waiting\" type=\"xs:string20\"/>\n");
    xml.append("<xs:element name=\"idlecount\" type=\"xs:string20\"/>\n");
    xml.append("<xs:element name=\"running_wuid1\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"running_wuid2\" type=\"xs:string\"/>\n");
    xml.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    xml.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    xml.append("</xs:schema>");
    xml.append("</XmlSchema>").newline();
    xml.append("<Dataset");
    xml.append(" name=\"data\" xmlSchema=\"MySchema\" ");
    xml.append(">").newline();

    StringBuffer filter("ThorQueueMonitor");
    if (notEmpty(cluster))
        filter.appendf(",%s", cluster);

    StringAttrArray lines;
    queryAuditLogs(fromTime, toTime, filter.str(), lines);

    unsigned longest = 0;
    unsigned maxConnected = 0;
    unsigned maxDisplay = 0;
    unsigned showVal = (showType && strieq(showType, "InGraph")) ? 1 : 0;

    IArrayOf<IEspThorQueue> items;
    WsWuJobQueueAuditInfo jq;
    ForEachItemIn(idx, lines)
    {
        const char* line = lines.item(idx).text;
        if(isEmpty(line))
            continue;
        bool isLast = (idx == (lines.length() - 1));
        jq.getAuditLineInfo(lines.item(idx).text, longest, maxConnected, maxDisplay, (isLast && showVal==1) ? 2 : showVal, items);
    }

    ForEachItemIn(i,items)
    {
        IEspThorQueue& tq = items.item(i);

        xml.append(" <Row>");
        appendXMLTag(xml, "datetime", tq.getDT());
        appendXMLTag(xml, "running", tq.getRunningWUs());
        appendXMLTag(xml, "queued", tq.getQueuedWUs());
        appendXMLTag(xml, "connected", tq.getConnectedThors());
        appendXMLTag(xml, "waiting", tq.getWaitingThors());
        appendXMLTag(xml, "idlecount", tq.getIdledThors());
        if (notEmpty(tq.getRunningWU1()))
            appendXMLTag(xml, "running_wuid1", tq.getRunningWU1());
        if (notEmpty(tq.getRunningWU2()))
            appendXMLTag(xml, "running_wuid2", tq.getRunningWU2());
        xml.append("</Row>");
        xml.newline();
    }

    xml.append("</Dataset>").newline();
    return true;
}

static const long LOGFILESIZELIMIT = 500000; //Limit page size to 500k

inline float timeStrToFloat(const char *timestr, float &val)
{
    if (!timestr)
        return val;

    int hours = 0;
    int mins = 0;

    while(isdigit(*timestr))
    {
        hours = 10 * hours + *timestr - '0';
        timestr++;
    }
    if(*timestr == ':')
        timestr++;
    while(isdigit(*timestr))
    {
        mins = 10 * mins + (*timestr - '0');
        timestr++;
    }

    val = hours + mins/60.0F;
    return val;
}

StringBuffer &getNextStrItem(StringBuffer &s, const char *&finger, const char delim=',')
{
    if (!finger)
        return s;
    const char *endp = strchr(finger, delim);
    if (endp)
    {
        s.append(endp - finger, finger);
        finger = endp+1;
    }
    else
    {
        s.append(finger);
        finger=NULL;
    }
    return s;
}

IEspECLJob* createEclJobFromAuditLine(double version, const char* str)
{
    if(isEmpty(str))
        return NULL;

    Owned<IEspECLJob> job = createECLJob("", "");

    StringBuffer sdate;
    getNextStrItem(sdate, str);
    if (sdate.length()>=9)
        sdate.setCharAt(10, 'T');
    if(!str)
        return job.getClear();

    StringBuffer s;
    getNextStrItem(s, str);
    if(!str)
        return job.getClear();
    getNextStrItem(s.clear(), str);
    if(!str)
        return job.getClear();

    job->setCluster(getNextStrItem(s.clear(), str).str());
    if(!str)
        return job.getClear();

    job->setWuid(getNextStrItem(s.clear(), str).str());
    if(!str)
        return job.getClear();

    getNextStrItem(s.clear(), str);
    if (version > 1.05)
        job->setGraphNum(s.str());
    StringBuffer graph("graph");
    job->setGraph(graph.append(s).str());
    if(!str)
        return job.getClear();

    getNextStrItem(s.clear(), str);
    if (version > 1.05)
        job->setSubGraphNum(s.str());
    if(!str)
        return job.getClear();

    getNextStrItem(s.clear(), str);
    if (version > 1.05)
        job->setNumOfRuns(s.str());
    if(!str)
        return job.getClear();

    getNextStrItem(s.clear(), str);
    if (version > 1.05)
       job->setDuration(atoi(s.str()) / 1000);
    if(!str)
        return job.getClear();

    if(!strncmp("FAILED", getNextStrItem(s.clear(), str).str(), 6))
        job->setState("failed");
    else
        job->setState("finished");


    CDateTime endDT;
    endDT.setString(sdate.str(), NULL, true);

    CDateTime startDT;
    startDT.set(endDT.getSimple() - job->getDuration());

    job->setStartedDate(startDT.getString(s.clear(), false).append('Z').str());
    job->setFinishedDate(endDT.getString(s.clear(), false).append('Z').str());

    return job.getClear();
}


bool getClusterJobXLS(double version, StringBuffer &xml, const char* cluster, const char* startDate, const char* endDate, bool showall, const char* busStartStr, const char* busEndStr)
{
    float busStart = 0;
    float busEnd = 24;

    if(showall)
    {
        timeStrToFloat(busStartStr, busStart);
        timeStrToFloat(busEndStr, busEnd);

        if(busStart <= 0 || busStart > 24 || busStart >= busEnd)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
    }

    CDateTime fromTime;
    if(notEmpty(startDate))
        fromTime.setString(startDate, NULL, false);
    CDateTime toTime;
    if(notEmpty(endDate))
        toTime.setString(endDate, NULL, false);

    xml.append("<XmlSchema name=\"MySchema\">");
    xml.append("<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");
    xml.append("<xs:element name=\"wuid\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"graph\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"sub-graph\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"runs\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"started\" type=\"xs:string20\"/>\n");
    xml.append("<xs:element name=\"finished\" type=\"xs:string20\"/>\n");
    xml.append("<xs:element name=\"duration\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"cluster\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"state\" type=\"xs:string\"/>\n");
    xml.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    xml.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    xml.append("</xs:schema>");
    xml.append("</XmlSchema>").newline();
    xml.append("<Dataset name=\"data\" xmlSchema=\"MySchema\">").newline();

    StringBuffer filter("Timing,ThorGraph");
    if(notEmpty(cluster))
        filter.append(',').append(cluster);

    StringAttrArray jobs;
    queryAuditLogs(fromTime, toTime, filter.str(), jobs);

    IArrayOf<IEspECLJob> jobList;
    ForEachItemIn(idx, jobs)
    {
        if(jobs.item(idx).text.isEmpty())
            continue;
        Owned<IEspECLJob> job = createEclJobFromAuditLine(version, jobs.item(idx).text.get());

        CDateTime tTime;
        unsigned year, month, day, hour, minute, second, nano;

        tTime.setString(job->getStartedDate(), NULL, true);
        tTime.getDate(year, month, day, true);
        tTime.getTime(hour, minute, second, nano, true);
        StringBuffer started;
        started.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);

        tTime.setString(job->getFinishedDate(), NULL, true);
        tTime.getDate(year, month, day, true);
        tTime.getTime(hour, minute, second, nano, true);
        StringBuffer finished;
        finished.appendf("%4d-%02d-%02d %02d:%02d:%02d",year,month,day,hour,minute,second);

        xml.append(" <Row>");
        appendXMLTag(xml, "wuid", job->getWuid());
        appendXMLTag(xml, "graph", job->getGraphNum());
        appendXMLTag(xml, "sub-graph", job->getSubGraphNum());
        appendXMLTag(xml, "runs", job->getNumOfRuns());
        appendXMLTag(xml, "started", started.str());
        appendXMLTag(xml, "finished", finished.str());
        xml.appendf("<duration>%d</duration>", job->getDuration());
        appendXMLTag(xml, "cluster", job->getCluster());
        appendXMLTag(xml, "state", job->getState());
        xml.append("</Row>").newline();

        jobList.append(*job.getClear());
    }
    xml.append("</Dataset>").newline();

    return true;
}

class CJobUsage : public CInterface
{
public:
    StringBuffer m_date;
    float m_usage, m_busage, m_nbusage;

public:
    IMPLEMENT_IINTERFACE;
    CJobUsage()
    {
        m_usage = 0.0;
        m_busage = 0.0;
        m_nbusage = 0.0;
    };
};

typedef CIArrayOf<CJobUsage> JobUsageArray;

//what???
int ZCMJD(unsigned y, unsigned m, unsigned d)
{
  if (m<3)
  {
      m += 12 ;
      y--;
  }

  return -678973 + d + (((153*m-2)/5)|0) + 365*y + ((y/4)|0) - ((y/100)|0) + ((y/400)|0);
}

void AddToClusterJobXLS(JobUsageArray& jobsummary, CDateTime &adjStart, CDateTime &adjEnd, int first, float busStart, float busEnd)
{
    unsigned year0, month0, day0, hour0, minute0, second0, nano0;
    adjStart.getDate(year0, month0, day0, true);
    adjStart.getTime(hour0, minute0, second0, nano0, true);

    unsigned year1, month1, day1, hour1, minute1, second1, nano1;
    adjEnd.getDate(year1, month1, day1, true);
    adjEnd.getTime(hour1, minute1, second1, nano1, true);

    busStart *= 3600.0;
    busEnd *= 3600.0;

    float x1 = 3600.0F*hour0 + 60.0F*minute0 + second0;
    int   y1 = ZCMJD(year0, month0, day0)-first;

    float x2 = 3600.0F*hour1 + 60.0F*minute1 + second1;
    int   y2 = ZCMJD(year1, month1, day1)-first;

    for(int y=y1; y<=y2; y++)
    {
        if ((y < 0) || (y > (int)jobsummary.length() - 1))
            continue;

        CJobUsage& jobUsage = jobsummary.item(y);

        float xx1= (y==y1 ? x1 : 0.0F);
        float xx2= (y==y2 ? x2 : 86400.0F);
        jobUsage.m_usage += (xx2-xx1)/864.0F;

        float bhours = ((busEnd < xx2)? busEnd : xx2) - ((busStart > xx1) ? busStart : xx1);
        if(bhours < 0.0)
            bhours = 0.0;
        float nbhours = (xx2 - xx1 - bhours);

        if(busStart + (86400.0 - busEnd) > 0.001)
            jobUsage.m_nbusage +=  100 * nbhours/(busStart + (86400.0F - busEnd));

        if(busEnd - busStart > 0.001)
            jobUsage.m_busage += 100*bhours/(busEnd - busStart);
    }

    return;
}

bool readECLWUCurrentJob(const char* curjob, const char* clusterName, const char* toDate, StringBuffer& dtStr, StringBuffer& actionStr, StringBuffer& wuidStr, StringBuffer& graphStr, StringBuffer& subGraphStr, StringBuffer& clusterStr)
{
    if(!curjob || !*curjob)
        return false;

    // startdate
    const char* bptr = curjob;
    const char* eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    StringBuffer dt;
    dt.clear().append(eptr - bptr, bptr);
    dt.setCharAt(10, 'T');

    if (strcmp(dt.str(), toDate) > 0)
        return false;

    CDateTime enddt;
    enddt.setString(dt.str(), NULL, true);
    dt.clear();
    enddt.getString(dt, false);
    dt.append('Z');

    //Progress
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    //Thor
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    // action name
    char action[256];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    int len = eptr - bptr;
    strncpy(action, bptr, len);
    action[len] = 0;

    // cluster name
    char cluster[256];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    len = eptr - bptr;
    strncpy(cluster, bptr, len);
    cluster[len] = 0;

    if (cluster && *cluster)
    {
        clusterStr.clear().append(cluster);
        if (clusterName && *clusterName && stricmp(cluster, clusterName))
            return false;
    }

    dtStr = dt;
    actionStr.clear().append(action);

    if (!stricmp(action, "startup") || !stricmp(action, "terminate"))
        return true;

    //WUID
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    wuidStr.clear().append(eptr - bptr, bptr);

    //graph number
    char graph[32];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
        return false;

    len = eptr - bptr;
    if (bptr[0] == 'g' && len > 5)
    {
        bptr += 5;
        len = eptr - bptr;
    }

    strncpy(graph, bptr, len);
    graph[len] = 0;
    graphStr.clear().append(eptr - bptr, bptr);

    if (!stricmp(action, "start") || !stricmp(action, "stop"))
        return true;

    //subgraph number
    char subgraph[32];
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
    {
        strcpy(subgraph, bptr);
    }
    else
    {
        len = eptr - bptr;
        strncpy(subgraph, bptr, len);
        subgraph[len] = 0;
    }
    subGraphStr.clear().append(subgraph);

    return true;
}

void addUnfinishedECLWUs(IArrayOf<IEspECLJob>& eclJobList, const char* wuid, const char* graph, const char* subGraph,
    const char* cluster, const char* dt, const char* dt1, StringArray& unfinishedWUIDs, StringArray& unfinishedGraphs,
    StringArray& unfinishedSubGraphs, StringArray& unfinishedClusters, StringArray& unfinishedWUStarttime, StringArray& unfinishedWUEndtime)
{
    bool bFound = false;
    ForEachItemIn(idx, eclJobList)
    {
        IConstECLJob& curECLJob = eclJobList.item(idx);
        const char *eclwuid = curECLJob.getWuid();
        const char *eclgraph = curECLJob.getGraphNum();
        const char *eclsubgraph = curECLJob.getSubGraphNum();
        const char *ecldate = curECLJob.getFinishedDate();
        if (!eclwuid || !*eclwuid || stricmp(eclwuid, wuid))
            continue;
        if (!eclgraph || !*eclgraph || stricmp(eclgraph, graph))
            continue;
        if (!eclsubgraph || !*eclsubgraph || stricmp(eclsubgraph, subGraph))
            continue;
        //if (!ecldate || !*ecldate || (stricmp(ecldate, dt) < 0) || (stricmp(ecldate, dt1) > 0))
        //  continue;
        if (!ecldate || !*ecldate)
            continue;
        int test = stricmp(ecldate, dt);
        if (test < 0)
            continue;
        test = stricmp(ecldate, dt1);
        if (test > 0)
            continue;

        bFound = true;
        break;
    }

    if (!bFound)
    {
        unfinishedWUIDs.append(wuid);
        StringBuffer graph0("graph");
        graph0.append(graph);
        unfinishedGraphs.append(graph0);
        unfinishedSubGraphs.append(subGraph);
        unfinishedClusters.append(cluster);
        unfinishedWUStarttime.append(dt);
        unfinishedWUEndtime.append(dt1);
    }

    return;
}

const unsigned MAXSUBGRAPHDAYS = 10;

bool getPreviousUnfinishedECLWU(CDateTime fromTime, CDateTime toTime, const char* toDate, const char* cluster,
    StringBuffer& wuidStr, StringBuffer& graphStr, StringBuffer& subGraphStr, StringBuffer& clusterStr, StringBuffer& dtStr)
{
    bool bFound = false;

    wuidStr.clear();
    graphStr.clear();
    subGraphStr.clear();
    dtStr.clear();

    StringBuffer filter0("Progress,Thor");
    CDateTime fromTime1 = fromTime, toTime1;

    bool bStop = false;
    for (unsigned day = 0; !bStop && day < MAXSUBGRAPHDAYS; day++)
    {
        toTime1 = fromTime1;
        fromTime1.adjustTime(-1440);

        StringAttrArray jobs1;
        queryAuditLogs(fromTime1, toTime1, filter0.str(), jobs1);
#if 0
        char* str1 = "2010-10-04 07:39:04 ,Progress,Thor,StartSubgraph,thor,W20100929-073902,5,1,thor,thor.thor";
        char* str2 = "2010-10-04 15:53:43 ,Progress,Thor,Startup,thor,thor,thor.thor,//10.173.51.20/c$/thor_logs/09_29_2010_15_52_39/THORMASTER.log";
        char* str3 = "2010-10-04 17:52:31 ,Progress,Thor,Start,thor,W20100929-075230,graph1,r3gression,thor,thor.thor";
        jobs1.append(*new StringAttrItem(str2, strlen(str2)));
        jobs1.append(*new StringAttrItem(str1, strlen(str1)));
        jobs1.append(*new StringAttrItem(str3, strlen(str3)));
#endif
        ForEachItemInRev(idx1, jobs1)
        {
            const char* curjob = jobs1.item(idx1).text;
            if(!curjob || !*curjob)
                continue;

            StringBuffer actionStr, clusterStr;
            if (!readECLWUCurrentJob(curjob, cluster, toDate, dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr))
                continue;

            if (!stricmp(actionStr.str(), "StartSubgraph") && (wuidStr.length() > 0) && (graphStr.length() > 0))
            {
                bFound = true;
                bStop = true;
                break;
            }

            if (!stricmp(actionStr.str(), "Startup") || !stricmp(actionStr.str(), "Terminate"))
            {
                bStop = true;
                break;
            }
        }
    }
    return bFound;
}


//Tony ToBeRefactored
void findUnfinishedECLWUs(IArrayOf<IEspECLJob>& eclJobList, CDateTime fromTime, CDateTime toTime, const char* toDate, const char* cluster, StringArray& unfinishedWUIDs,
    StringArray& unfinishedGraphs, StringArray& unfinishedSubGraphs, StringArray& unfinishedClusters, StringArray& unfinishedWUStarttime, StringArray& unfinishedWUEndtime)
{
    StringAttrArray jobs1;
    StringBuffer filter1("Progress,Thor");
    queryAuditLogs(fromTime, toTime, filter1.str(), jobs1);

    bool bAbnormalWU = false;
    int len = jobs1.length();

    StringBuffer dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr;
    ForEachItemIn(idx1, jobs1)
    {
        const char* curjob = jobs1.item(idx1).text;
        if(!curjob || !*curjob)
            continue;

        wuidStr.clear();
        graphStr.clear();
        subGraphStr.clear();

        if (!readECLWUCurrentJob(curjob, cluster, toDate, dtStr, actionStr, wuidStr, graphStr, subGraphStr, clusterStr))
            continue;

        if (stricmp(actionStr.str(), "Start"))
            continue;

        bAbnormalWU = true;
        int nextIndex = idx1 + 1;
        int idx2 = nextIndex;
        while (idx2 < len)
        {
            const char* curjob1 = jobs1.item(idx2).text;
            if(!curjob1 || !*curjob1)
                continue;

            StringBuffer dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1;
            if (readECLWUCurrentJob(curjob1, cluster, toDate, dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1))
            {
                if (!stricmp(wuidStr.str(), wuidStr1.str()) && !stricmp(graphStr.str(), graphStr1.str()))
                {
                    if (!stricmp(actionStr1.str(), "Stop") )
                    {
                        bAbnormalWU = false;
                        break;
                    }
                    else if (!stricmp(actionStr1.str(), "Start"))
                    {
                        break;
                    }
                }
            }

            idx2++;
        }

        //If the WU did not finish by itself, let's check whether the cluster was stopped before the WU finished.
        if (bAbnormalWU)
        {
            int idx2 = nextIndex;
            while (idx2 < len)
            {
                const char* curjob1 = jobs1.item(idx2).text;
                if(!curjob1 || !*curjob1)
                    continue;

                StringBuffer dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1;
                if (!readECLWUCurrentJob(curjob1, cluster, toDate, dtStr1, actionStr1, wuidStr1, graphStr1, subGraphStr1, clusterStr1))
                {
                    idx2++;
                    continue;
                }

                if (!stricmp(actionStr1.str(), "StartSubgraph") && !stricmp(wuidStr.str(), wuidStr1.str()) && !stricmp(graphStr.str(), graphStr1.str()))
                {
                    //update subgraph number
                    subGraphStr.clear().append(subGraphStr1.str());
                    dtStr.clear().append(dtStr1.str());
                    clusterStr.clear().append(clusterStr1.str());
                    idx2++;
                    continue;
                }
                if (stricmp(actionStr1.str(), "Startup") && stricmp(actionStr1.str(), "Terminate"))
                {
                    idx2++;
                    continue;
                }

                addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), dtStr1.str(),
                    unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

                bAbnormalWU = false;
                break;
            }

            if (bAbnormalWU)
            {
                addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), toDate,
                    unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);
                bAbnormalWU = false;
            }
        }
    }

    //What if a WU started before *and* ended after the search time range
    if ((eclJobList.length() < 1) && (unfinishedWUIDs.length() < 1))
    {
        if (getPreviousUnfinishedECLWU(fromTime, toTime, toDate, cluster, wuidStr, graphStr, subGraphStr, clusterStr, dtStr))
        {
            addUnfinishedECLWUs(eclJobList, wuidStr.str(), graphStr.str(), subGraphStr.str(), clusterStr.str(), dtStr.str(), toDate,
                unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);
        }
    }
    return;
}

bool getClusterJobSummaryXLS(double version, StringBuffer &xml, const char* cluster, const char* startDate, const char* endDate, bool showall, const char* busStartStr, const char* busEndStr)
{
    float busStart = 0;
    float busEnd = 24;
    if(showall)
    {
        timeStrToFloat(busStartStr, busStart);
        timeStrToFloat(busEndStr, busEnd);

        if(busStart <= 0 || busStart > 24 || busStart >= busEnd)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
    }

    CDateTime fromTime;
    if(notEmpty(startDate))
        fromTime.setString(startDate, NULL, false);
    CDateTime toTime;
    int delayTime = -240; //4 hour time difference
    bool extendedToNextDay = false;
    if(notEmpty(endDate))
    {
        toTime.setString(endDate, NULL, false);
        unsigned year, month, day, day1;
        CDateTime tTime = toTime;
        tTime.getDate(year, month, day);
        tTime.adjustTime(delayTime);
        tTime.getDate(year, month, day1);
        if (day1 < day)
            extendedToNextDay = true;
    }

    StringAttrArray jobs;
    StringBuffer filter("Timing,ThorGraph");
    if(notEmpty(cluster))
        filter.append(',').append(cluster);
    queryAuditLogs(fromTime, toTime, filter.str(), jobs);

    unsigned year, month, day;
    fromTime.getDate(year, month, day);
    int first = ZCMJD(year, month, day);
    toTime.getDate(year, month, day);
    int last = ZCMJD(year, month, day);
    if (last < first)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid day range");

    CDateTime usageDT(fromTime);
    JobUsageArray jobUsages;
    for (int i = first; i <= last; i++)
    {
        Owned<CJobUsage> jobUsage =  new CJobUsage();
        usageDT.getDateString(jobUsage->m_date);
        jobUsages.append(*jobUsage.getClear());
        usageDT.adjustTime(60*24);
    }

    IArrayOf<IEspECLJob> jobList;
    ForEachItemIn(idx, jobs)
    {
        if(jobs.item(idx).text.isEmpty())
            continue;

        Owned<IEspECLJob> job = createEclJobFromAuditLine(version, jobs.item(idx).text.get());

        CDateTime adjStart;
        adjStart.setString(job->getStartedDate(),NULL,true);
        adjStart.adjustTime(delayTime);

        CDateTime adjEnd;
        adjEnd.setString(job->getFinishedDate(),NULL,true);
        adjEnd.adjustTime(delayTime);

        AddToClusterJobXLS(jobUsages, adjStart, adjEnd, first, busStart, busEnd);

        jobList.append(*job.getClear());
    }

    StringBuffer s;
    StringArray unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime;
    findUnfinishedECLWUs(jobList, fromTime, toTime, toTime.getString(s).str(), cluster, unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);

    ForEachItemIn(idx3, unfinishedWUIDs)
    {
        CDateTime adjStart;
        adjStart.setString(unfinishedWUStarttime.item(idx3), NULL, true);
        adjStart.adjustTime(delayTime);

        CDateTime adjEnd;
        adjEnd.setString(unfinishedWUEndtime.item(idx3),NULL,true);
        adjEnd.adjustTime(delayTime);

        AddToClusterJobXLS(jobUsages, adjStart, adjEnd, first, busStart, busEnd);
    }

    xml.append("<XmlSchema name=\"MySchema\">");
    xml.append(
    "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" elementFormDefault=\"qualified\" attributeFormDefault=\"unqualified\">\n"
        "<xs:element name=\"Dataset\">"
            "<xs:complexType>"
                "<xs:sequence minOccurs=\"0\" maxOccurs=\"unbounded\">\n"
                    "<xs:element name=\"Row\">"
                        "<xs:complexType>"
                            "<xs:sequence>\n");

    xml.append("<xs:element name=\"Date\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"Business\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"Non-business\" type=\"xs:string\"/>\n");
    xml.append("<xs:element name=\"Overall\" type=\"xs:string\"/>\n");
    xml.append(                "</xs:sequence>"
                        "</xs:complexType>"
                    "</xs:element>\n"
                "</xs:sequence>"
            "</xs:complexType>"
        "</xs:element>\n");
    xml.append("<xs:simpleType name=\"string20\"><xs:restriction base=\"xs:string\"><xs:maxLength value=\"20\"/></xs:restriction></xs:simpleType>\n");
    xml.append("</xs:schema>");
    xml.append("</XmlSchema>").newline();
    xml.append("<Dataset name=\"data\" xmlSchema=\"MySchema\" >").newline();

    StringBuffer percentageStr;
    percentageStr.append("%");
    int lastUsage = jobUsages.length();
    if (extendedToNextDay)
        lastUsage --;
    for (int i0 = 0; i0 < lastUsage; i0++)
    {
        CJobUsage& jobUsage = jobUsages.item(i0);
        xml.append(" <Row>");
        xml.appendf("<Date>%s</Date>", jobUsage.m_date.str());
        xml.appendf("<Business>%3.0f %s</Business>", jobUsage.m_busage, percentageStr.str());
        xml.appendf("<Non-business>%3.0f %s</Non-business>", jobUsage.m_nbusage, percentageStr.str());
        xml.appendf("<Overall>%3.0f %s</Overall>", jobUsage.m_usage, percentageStr.str());
        xml.append(" </Row>");
    }
    xml.append("</Dataset>").newline();
    return true;
}




inline StringBuffer &appendQuoted(StringBuffer &s, const char *val, bool first=false, const char q='\'')
{
    if (!first)
        s.append(',');
    return s.append(q).append(val).append(q);
}

inline StringBuffer &appendQuoted(StringBuffer &s, float val, bool first=false, const char q='\'')
{
    if (!first)
        s.append(',');
    return s.append(q).append(val).append(q);
}

inline StringBuffer &appendQuoted(StringBuffer &s, unsigned val, bool first=false, const char q='\'')
{
    if (!first)
        s.append(',');
    return s.append(q).append(val).append(q);
}

void streamJobListResponse(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, bool showall, float bbtime, float betime, const char *xls)
{
    CDateTime fromTime;
    StringBuffer fromstr;
    if (notEmpty(from))
    {
        fromTime.setString(from, NULL, false);
        fromTime.getString(fromstr, false);
    }

    CDateTime toTime;
    StringBuffer tostr;
    if(notEmpty(to))
    {
        toTime.setString(to,NULL,false);
        toTime.getString(tostr, false);
    }

    StringBuffer sb;
    sb.append("<script language=\"javascript\">parent.displayLegend(");
    appendQuoted(sb, fromstr.str(), true);
    appendQuoted(sb, tostr.str());
    appendQuoted(sb, showall ? "1" : "0");
    sb.append(")</script>\r\n");
    response->sendChunk(sb.str());
    sb.clear();

    sb.append("<script language=\"javascript\">parent.displayBegin(");
    appendQuoted(sb, fromstr.str(), true);
    appendQuoted(sb, tostr.str());
    appendQuoted(sb, showall ? "1" : "0");
    sb.append(")</script>\r\n");
    response->sendChunk(sb.str());
    sb.clear();

    StringBuffer filter("Timing,ThorGraph");
    if(notEmpty(cluster))
        filter.appendf(",%s", cluster);
    StringAttrArray jobs;
    queryAuditLogs(fromTime, toTime, filter.str(), jobs);

    sb.append("<script language=\"javascript\">\r\n");

    unsigned count=0;
    IArrayOf<IEspECLJob> eclJobList;
    ForEachItemIn(idx, jobs)
    {
        if(!jobs.item(idx).text.length())
            continue;
        Owned<IEspECLJob> job = createEclJobFromAuditLine(context.getClientVersion(), jobs.item(idx).text.get());

        sb.append("parent.displayJob(");
        appendQuoted(sb, job->getWuid(), true);
        appendQuoted(sb, job->getGraph());
        appendQuoted(sb, job->getStartedDate());
        appendQuoted(sb, job->getFinishedDate());
        appendQuoted(sb, job->getCluster());
        appendQuoted(sb, job->getState());
        if (showall)
            sb.append(",\'\',\'1\'");
        else
            sb.append(",\'\',\'0\'");
        sb.append(',').append(bbtime).append(',').append(betime).append(")\r\n");
        if(++count>=50)
        {
            sb.append("</script>\r\n");
            response->sendChunk(sb.str());
            sb.clear().append("<script language=\"javascript\">\r\n");
            count=0;
        }
        eclJobList.append(*job.getClear());
    }

    //Find out which WUs stopped by abnormal thor termination
    StringArray unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime;
    findUnfinishedECLWUs(eclJobList, fromTime, toTime, tostr.str(), cluster, unfinishedWUIDs, unfinishedGraphs, unfinishedSubGraphs, unfinishedClusters, unfinishedWUStarttime, unfinishedWUEndtime);
    if (unfinishedWUIDs.ordinality())
    {
        ForEachItemIn(idx3, unfinishedWUIDs)
        {
            CDateTime tTime;
            unsigned year, month, day, hour, minute, second, nano;
            tTime.setString(unfinishedWUStarttime.item(idx3),NULL,true);
            tTime.getDate(year, month, day, true);
            tTime.getTime(hour, minute, second, nano, true);

            StringBuffer started, finished;
            started.appendf("%4d-%02d-%02dT%02d:%02d:%02dZ",year,month,day,hour,minute,second);
            if (notEmpty(unfinishedWUEndtime.item(idx3)))
            {
                tTime.setString(unfinishedWUEndtime.item(idx3),NULL,true);
                tTime.getDate(year, month, day, true);
                tTime.getTime(hour, minute, second, nano, true);
                finished.appendf("%4d-%02d-%02dT%02d:%02d:%02dZ",year,month,day,hour,minute,second);
            }

            sb.append("parent.displayJob(");
            appendQuoted(sb, unfinishedWUIDs.item(idx3), true);
            appendQuoted(sb, unfinishedGraphs.item(idx3));
            appendQuoted(sb, started.str());
            appendQuoted(sb, finished.str());
            appendQuoted(sb, unfinishedClusters.item(idx3));
            appendQuoted(sb, "not finished");
            if (showall)
                sb.append(",\'\',\'1\'");
            else
                sb.append(",\'\',\'0\'");
            sb.append(',').append(bbtime).append(',').append(betime).append(")\r\n");

            if(++count>=50)
            {
                sb.append("</script>\r\n");
                response->sendChunk(sb.str());
                sb.clear().append("<script language=\"javascript\">\r\n");
                count=0;
            }
        }
    }
    sb.append("</script>\r\n");
    response->sendChunk(sb.str());
    sb.clear().append("<script language=\"javascript\">\r\n");
    sb.append("parent.displaySasha();\r\nparent.displayEnd(");
    appendQuoted(sb, xls, true).append(")</script>\r\n");
    response->sendChunk(sb.str());
}

bool checkSameStrings(const char* s1, const char* s2)
{
    if (s1)
    {
        if (s2 && streq(s1, s2))
            return true;
    }
    else if (!s2)
        return true;
    return false;
}


bool checkNewThorQueueItem(IEspThorQueue* tq, unsigned showAll, IArrayOf<IEspThorQueue>& items)
{
    bool bAdd = false;
    if (showAll < 1) //show every lines
        bAdd = true;
    else if (items.length() < 1)
        bAdd = true;
    else if (showAll > 1) //last line now
    {
        IEspThorQueue& tq0 = items.item(items.length()-1);
        if (!checkSameStrings(tq->getDT(), tq0.getDT()))
            bAdd = true;
    }
    else
    {
        IEspThorQueue& tq0 = items.item(items.length()-1);
        if (!checkSameStrings(tq->getRunningWUs(), tq0.getRunningWUs()))
            bAdd = true;
        if (!checkSameStrings(tq->getQueuedWUs(), tq0.getQueuedWUs()))
            bAdd = true;
        if (!checkSameStrings(tq->getConnectedThors(), tq0.getConnectedThors()))
            bAdd = true;
        if (!checkSameStrings(tq->getConnectedThors(), tq0.getConnectedThors()))
            bAdd = true;
        if (!checkSameStrings(tq->getRunningWU1(), tq0.getRunningWU1()))
            bAdd = true;
        if (!checkSameStrings(tq->getRunningWU2(), tq0.getRunningWU2()))
            bAdd = true;
    }

    return bAdd;
}

void appendQueueInfoFromAuditLine(IArrayOf<IEspThorQueue>& items, const char* line, unsigned& longestQueue, unsigned& maxConnected, unsigned maxDisplay, unsigned showAll)
{
    //2009-08-12 02:44:12 ,ThorQueueMonitor,thor400_88_dev,0,0,1,1,114,---,---
    if(!line || !*line)
        return;

    Owned<IEspThorQueue> tq = createThorQueue();
    StringBuffer dt, runningWUs, queuedWUs, waitingThors, connectedThors, idledThors, runningWU1, runningWU2;

    // date/time
    const char* bptr = line;
    const char* eptr = strchr(bptr, ',');
    if(eptr)
        dt.append(eptr - bptr, bptr);
    else
        dt.append(bptr);

    tq->setDT(dt.str());
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //skip title
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //skip queue name
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //running
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        runningWUs.append(eptr - bptr, bptr);
    else
        runningWUs.append(bptr);

    tq->setRunningWUs(runningWUs.str());
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //queued
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        queuedWUs.append(eptr - bptr, bptr);
    else
        queuedWUs.append(bptr);

    if (maxDisplay > items.length())
    {
        unsigned queueLen = atoi(queuedWUs.str());
        if (queueLen > longestQueue)
            longestQueue = queueLen;
    }

    tq->setQueuedWUs(queuedWUs.str());
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //waiting
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        waitingThors.append(eptr - bptr, bptr);
    else
        waitingThors.append(bptr);

    tq->setWaitingThors(waitingThors.str());
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //connected
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        connectedThors.append(eptr - bptr, bptr);
    else
        connectedThors.append(bptr);

    if (maxDisplay > items.length())
    {
        unsigned connnectedLen = atoi(connectedThors.str());
        if (connnectedLen > maxConnected)
            maxConnected = connnectedLen;
    }

    tq->setConnectedThors(connectedThors.str());
    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //idled
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        idledThors.append(eptr - bptr, bptr);
    else
        idledThors.append(bptr);

    tq->setIdledThors(idledThors.str());
    if(!eptr)
    {
        items.append(*tq.getClear());
        return;
    }

    //runningWU1
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        runningWU1.append(eptr - bptr, bptr);
    else
    {
        runningWU1.append(bptr);
    }

    if (!strcmp(runningWU1.str(), "---"))
        runningWU1.clear();

    if (runningWU1.length() > 0)
        tq->setRunningWU1(runningWU1.str());

    if(!eptr)
    {
        if (checkNewThorQueueItem(tq, showAll, items))
            items.append(*tq.getClear());
        return;
    }

    //runningWU2
    bptr = eptr + 1;
    eptr = strchr(bptr, ',');
    if(eptr)
        runningWU2.append(eptr - bptr, bptr);
    else
    {
        runningWU2.append(bptr);
    }

    if (!strcmp(runningWU2.str(), "---"))
        runningWU2.clear();

    if (runningWU2.length() > 0)
        tq->setRunningWU2(runningWU2.str());

    if (checkNewThorQueueItem(tq, showAll, items))
        items.append(*tq.getClear());

    DBGLOG("Queue log: [%s]", line);
}


void streamJobQueueListResponse(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, const char *xls)
{
    if(!response)
        return;

    unsigned maxDisplay = 125;
    IArrayOf<IEspThorQueue> items;

    CDateTime fromTime;
    StringBuffer fromstr;
    if (notEmpty(from))
    {
        fromTime.setString(from, NULL, false);
        fromTime.getString(fromstr, false);
    }

    CDateTime toTime;
    StringBuffer tostr;
    if(notEmpty(to))
    {
        toTime.setString(to,NULL,false);
        toTime.getString(tostr, false);
    }

    StringBuffer filter("ThorQueueMonitor");
    if (notEmpty(cluster))
        filter.appendf(",%s", cluster);

    StringAttrArray lines;
    queryAuditLogs(fromTime, toTime, filter.str(), lines);

    unsigned countLines = 0;
    unsigned maxConnected = 0;
    unsigned longestQueue = 0;
    ForEachItemIn(idx, lines)
    {
        if(!lines.item(idx).text.length())
            continue;
        if (idx < (lines.length() - 1))
            appendQueueInfoFromAuditLine(items, lines.item(idx).text.get(), longestQueue, maxConnected, maxDisplay, 1);
        else
            appendQueueInfoFromAuditLine(items, lines.item(idx).text.get(), longestQueue, maxConnected, maxDisplay, 2);
        countLines++;
    }

    if (items.length() < 1)
    {
        response->sendChunk("<script language=\"javascript\">\r\nparent.displayQEnd(\'No data found\')</script>\r\n");
        return;
    }

    unsigned itemCount = items.length();
    if (itemCount > maxDisplay)
        itemCount = maxDisplay;

    StringBuffer sb;
    sb.append("<script language=\"javascript\">parent.displayQLegend()</script>\r\n");
    sb.append("<script language=\"javascript\">parent.displayQBegin(");
    sb.append(longestQueue).append(',').append(maxConnected).append(',').append(itemCount).append(")</script>\r\n");
    response->sendChunk(sb.str());

    sb.clear().append("<script language=\"javascript\">\r\n");

    unsigned count = 0;
    unsigned sbcount=0;
    ForEachItemIn(i,items)
    {
        IEspThorQueue& tq = items.item(i);
        count++;
        if (count > maxDisplay)
            break;

        sb.append("parent.displayQueue(");
        appendQuoted(sb, count, true);
        appendQuoted(sb, tq.getDT());
        appendQuoted(sb, tq.getRunningWUs());
        appendQuoted(sb, tq.getQueuedWUs());
        appendQuoted(sb, tq.getWaitingThors());
        appendQuoted(sb, tq.getConnectedThors());
        appendQuoted(sb, tq.getIdledThors());
        appendQuoted(sb, tq.getRunningWU1());
        appendQuoted(sb, tq.getRunningWU2());
        sb.append(")\r\n");
        if(++sbcount>=50)
        {
            sb.append("</script>\r\n");
            response->sendChunk(sb.str());
            sb.clear().append("<script language=\"javascript\">\r\n");
            sbcount=0;
        }
    }

    sb.append("parent.displayQEnd(\'<table><tr><td>");
    sb.append("Total Records in the Time Period: ").append(items.length());
    sb.append(" (<a href=\"/WsWorkunits/WUClusterJobQueueLOG?").append(xls);
    sb.append("\">txt</a>...<a href=\"/WsWorkunits/WUClusterJobQueueXLS?").append(xls).append("\">xls</a>).");
    sb.append("</td></tr><tr><td>");
    if (count > maxDisplay)
        sb.append("Displayed: First ").append(maxDisplay).append(". ");
    sb.append("Max. Queue Length: ").append(longestQueue).append(".");
    sb.append("</td></tr></table>\')</script>\r\n");

    response->sendChunk(sb.str());
}

int CWsWorkunitsSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    IEspContext *ctx = request->queryContext();
    IProperties *params = request->queryParameters();


    try
    {
         StringBuffer path;
         request->getPath(path);

         if(!strnicmp(path.str(), "/WsWorkunits/res/", strlen("/WsWorkunits/res/")))
         {
            const char *pos = path.str();
            StringBuffer wuid;
            nextPathNode(pos, wuid, 2);
            Owned<IWuWebView> web = createWuWebView(wuid, wuid, getCFD(), true);
            if (!web)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Cannot open workunit");
            MemoryBuffer mb;
            StringAttr mimetype(mimeTypeFromFileExt(strrchr(pos, '.')));
            if (!web->getResourceByPath(pos, mb))
                throw MakeStringException(ECLWATCH_RESOURCE_NOT_FOUND, "Cannot open resource");

            response->setContent(mb.length(), mb.toByteArray());
            response->setContentType(mimetype.get());
            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return 0;
         }
         if(!strnicmp(path.str(), "/WsWorkunits/manifest/", strlen("/WsWorkunits/manifest/")))
         {
            const char *pos = path.str();
            StringBuffer wuid;
            nextPathNode(pos, wuid, 2);
            Owned<IWuWebView> web = createWuWebView(wuid, wuid, getCFD(), true);
            if (!web)
                throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Cannot open workunit");
            StringBuffer mf;
            if (!web->getManifest(mf).length())
                throw MakeStringException(ECLWATCH_RESOURCE_NOT_FOUND, "Cannot open manifest");

            response->setContent(mf.str());
            response->setContentType("text/xml");
            response->setStatus(HTTP_STATUS_OK);
            response->send();
            return 0;
         }
         if(!strnicmp(path.str(), "/WsWorkunits/JobList", 20))
         {
            const char *cluster = params->queryProp("Cluster");
            const char *startDate = params->queryProp("StartDate");
            const char *endDate = params->queryProp("EndDate");
            const char *showAll = params->queryProp("ShowAll");
            const char *busStart = params->queryProp("BusinessStartTime");
            const char *busEnd = params->queryProp("BusinessEndTime");

            float fBusStart = 0;
            float fBusEnd = 24;

            bool bShowAll = (isEmpty(showAll) ? false : (atoi(showAll)==1));
            if (bShowAll)
            {
                timeStrToFloat(busStart, fBusStart);
                timeStrToFloat(busEnd, fBusEnd);

                if(fBusStart <= 0 || fBusEnd > 24 || fBusStart >= fBusEnd)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid business hours");
            }

            response->addHeader("Expires", "0");
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);

            StringBuffer xls("ShowAll=1");
            addToQueryString(xls, "Cluster", cluster);
            addToQueryString(xls, "StartDate", startDate);
            addToQueryString(xls, "EndDate", endDate);
            addToQueryString(xls, "BusinessStartTime", busStart);
            addToQueryString(xls, "BusinessEndTime", busEnd);

            streamJobListResponse(*ctx, cluster, startDate, endDate, response, bShowAll, fBusStart, fBusEnd, xls.str());
            return 0;
        }
        else if(!strnicmp(path.str(), "/WsWorkunits/JobQueue", 21))
        {
            const char *cluster = params->queryProp("Cluster");
            const char *startDate = params->queryProp("StartDate");
            const char *endDate = params->queryProp("EndDate");

            response->addHeader("Expires", "0");
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);

            StringBuffer xls;
            xls.append("ShowType=InGraph");
            addToQueryString(xls, "Cluster", cluster);
            addToQueryString(xls, "StartDate", startDate);
            addToQueryString(xls, "EndDate", endDate);

            streamJobQueueListResponse(*ctx, cluster, startDate, endDate, response, xls.str());
            return 0;
        }
    }
    catch(IException* e)
    {
        onGetException(*request->queryContext(), request, response, *e);
        FORWARDEXCEPTION(*request->queryContext(), e,  ECLWATCH_INTERNAL_ERROR);
    }

    return CWsWorkunitsSoapBinding::onGet(request,response);
}

bool CWsWorkunitsEx::onWUClusterJobQueueXLS(IEspContext &context, IEspWUClusterJobQueueXLSRequest &req, IEspWUClusterJobQueueXLSResponse &resp)
{
    try
    {
        DBGLOG("WUClusterJobQueueXLS");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        getUserWuAccessFlags(context, accessOwn, accessOthers, true);

        StringBuffer xml("<WUResultExcel><Result>");
        getClusterJobQueueXLS(xml, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowType());
        xml.append("</Result></WUResultExcel>");

        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);
        StringBuffer xls;
        xsltTransform(xml.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer mb;
        mb.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(mb);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUClusterJobQueueLOG(IEspContext &context,IEspWUClusterJobQueueLOGRequest  &req, IEspWUClusterJobQueueLOGResponse &resp)
{
    try
    {
        DBGLOG("WUClusterJobQueueLOG");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        getUserWuAccessFlags(context, accessOwn, accessOthers, true);

        CDateTime fromTime;
        if(notEmpty(req.getStartDate()))
            fromTime.setString(req.getStartDate(), NULL, false);
        CDateTime toTime;
        if(notEmpty(req.getEndDate()))
            toTime.setString(req.getEndDate(), NULL, false);

        const char *cluster = req.getCluster();
        StringBuffer filter("ThorQueueMonitor");
        if (notEmpty(cluster))
            filter.appendf(",%s", cluster);

        StringAttrArray lines;
        queryAuditLogs(fromTime, toTime, filter.str(), lines);

        StringBuffer text;
        ForEachItemIn(idx, lines)
        {
            if (lines.item(idx).text.isEmpty())
                continue;
            text.append(lines.item(idx).text.get()).append("\r\n");
            if (text.length()>LOGFILESIZELIMIT)
            {
                text.appendf("... ...");
                break;
            }
        }

        MemoryBuffer mb;
        mb.setBuffer(text.length(), (void*)text.str());
        resp.setThefile(mb);
        resp.setThefile_mimetype(HTTP_TYPE_TEXT_PLAIN);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUJobList(IEspContext &context, IEspWUJobListRequest &req, IEspWUJobListResponse &resp)
{
   return true;
}

bool CWsWorkunitsEx::onWUClusterJobXLS(IEspContext &context, IEspWUClusterJobXLSRequest &req, IEspWUClusterJobXLSResponse &resp)
{
    try
    {
        DBGLOG("WUClusterJobXLS");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        getUserWuAccessFlags(context, accessOwn, accessOthers, true);

        StringBuffer xml("<WUResultExcel><Result>");
        getClusterJobXLS(context.getClientVersion(), xml, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowAll(), req.getBusinessStartTime(), req.getBusinessEndTime());
        xml.append("</Result></WUResultExcel>");
        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);

        StringBuffer xls;
        xsltTransform(xml.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer mb;
        mb.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(mb);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUClusterJobSummaryXLS(IEspContext &context, IEspWUClusterJobSummaryXLSRequest &req, IEspWUClusterJobSummaryXLSResponse &resp)
{
    try
    {
        DBGLOG("WUClusterJobSummaryXLS");

        SecAccessFlags accessOwn;
        SecAccessFlags accessOthers;
        getUserWuAccessFlags(context, accessOwn, accessOthers, true);

        StringBuffer xml("<WUResultExcel><Result>");
        getClusterJobSummaryXLS(context.getClientVersion(), xml, req.getCluster(), req.getStartDate(), req.getEndDate(), req.getShowAll(), req.getBusinessStartTime(), req.getBusinessEndTime());
        xml.append("</Result></WUResultExcel>");

        StringBuffer xls;
        Owned<IProperties> params(createProperties());
        params->setProp("showCount",0);
        xsltTransform(xml.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

        MemoryBuffer mb;
        mb.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(mb);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
