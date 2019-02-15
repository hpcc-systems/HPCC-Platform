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


#include "platform.h"
#include "build-config.h"

#include <algorithm>
#include "stdio.h"
#include "jlog.hpp"
#include "jlog.ipp"
#include "jmutex.hpp"
#include "jarray.hpp"
#include "jsocket.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"

#include "libbase58.h"

#define MSGCOMP_NUMBER 1000
#define FILE_LOG_ENABLES_QUEUEUING

#ifndef _WIN32
#define AUDIT_DATA_LOG_TEMPLATE "/var/log/seisint/log_data_XXXXXX"
#endif

// Time, in nanoseconds, after which the clock field loops --- 3600000000000ns = 1hr
#define CLOCK_LOOP_NANOSECONDS I64C(3600000000000)

// LogMsgSysInfo

static FILE *getNullHandle()
{
#ifdef _WIN32
    return fopen("nul","w");
#else
    return fopen("/dev/null","w");
#endif
}

LogMsgSysInfo::LogMsgSysInfo(LogMsgId _id, unsigned port, LogMsgSessionId session)
{
    id = _id;
#ifdef _WIN32
    // Hack for the fact that Windows doesn't handle gettimeofday
    // Subsecond timing granularities in log files will not be available
    time(&timeStarted);
#else
    gettimeofday(&timeStarted, NULL);
#endif
    processID = GetCurrentProcessId();
    threadID = threadLogID();
    sessionID = session;
    node.setLocalHost(port);
}

void LogMsgSysInfo::serialize(MemoryBuffer & out) const
{
    out.append(id).append((unsigned) queryTime()).append(processID).append(threadID).append(sessionID); node.serialize(out);
}

void LogMsgSysInfo::deserialize(MemoryBuffer & in)
{
    unsigned t;
    in.read(id).read(t).read(processID).read(threadID).read(sessionID); node.deserialize(in);
#ifdef _WIN32
    timeStarted = t;
#else
    timeStarted.tv_sec = t;
    timeStarted.tv_usec = 0;  // For back-compatibility reasons, the subsecond timings are not serialized
#endif
}

// LogMsg

StringBuffer & LogMsg::toStringPlain(StringBuffer & out, unsigned fields) const
{
    out.ensureCapacity(LOG_MSG_FORMAT_BUFFER_LENGTH);
    if(fields & MSGFIELD_audience)
        out.append("aud=").append(LogMsgAudienceToVarString(category.queryAudience())).append(' ');
    if(fields & MSGFIELD_class)
        out.append("cls=").append(LogMsgClassToVarString(category.queryClass())).append(' ');
    if(fields & MSGFIELD_detail)
        out.appendf("det=%d ", category.queryDetail());
    if(fields & MSGFIELD_msgID)
        out.appendf("id=%X ", sysInfo.queryMsgID());
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[12];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 12, "%Y-%m-%d ", &timeStruct);
            out.append(timeString);
        }
        if(fields & MSGFIELD_microTime)
        {
            out.appendf("%02d:%02d:%02d.%06d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            out.appendf("%02d:%02d:%02d.%03d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 12, "%H:%M:%S ", &timeStruct);
            out.append(timeString);
        }
    }
    if(fields & MSGFIELD_process)
        out.appendf("pid=%d ",sysInfo.queryProcessID());
    if(fields & MSGFIELD_thread)
        out.appendf("tid=%d ",sysInfo.queryThreadID());
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            out.append("sid=unknown ");
        else
            out.appendf("sid=%" I64F "u ", sysInfo.querySessionID());
    }
    if(fields & MSGFIELD_node)
    {
        sysInfo.queryNode()->getUrlStr(out);
        out.append(" ");
    }
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            out.append("job=unknown ");
        else
            out.appendf("job=%" I64F "u ", jobInfo.queryJobID());
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            out.append("usr=unknown ");
        else
            out.appendf("usr=%" I64F "u ", jobInfo.queryUserID());
    }
    if(fields & MSGFIELD_component)
        out.appendf("cmp=%u ", component);
    if (fields & MSGFIELD_quote)
        out.append('"');
    if (fields & MSGFIELD_prefix)
        out.append(msgPrefix(category.queryClass()));
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        out.append(msgCode).append(": ").append(text.str());
    else
        out.append(text.str());
    if (fields & MSGFIELD_quote)
        out.append('"');
    return out;
}

StringBuffer & LogMsg::toStringXML(StringBuffer & out, unsigned fields) const
{
    out.ensureCapacity(LOG_MSG_FORMAT_BUFFER_LENGTH);
    out.append("<msg ");
    if(fields & MSGFIELD_audience)
        out.append("Audience=\"").append(LogMsgAudienceToVarString(category.queryAudience())).append("\" ");
    if(fields & MSGFIELD_class)
        out.append("Class=\"").append(LogMsgClassToVarString(category.queryClass())).append("\" ");
    if(fields & MSGFIELD_detail)
        out.append("Detail=\"").append(category.queryDetail()).append("\" ");
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allCategory) out.append("\n     ");
#endif
    if(fields & MSGFIELD_msgID)
        out.append("MessageID=\"").append(sysInfo.queryMsgID()).append("\" ");
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[20];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 20, "date=\"%Y-%m-%d\" ", &timeStruct);
            out.append(timeString);
        }
        if(fields & MSGFIELD_microTime)
        {
            out.appendf("time=\"%02d:%02d:%02d.%06d\" ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            out.appendf("time=\"%02d:%02d:%02d.%03d\" ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 20, "time=\"%H:%M:%S\" ", &timeStruct);
            out.append(timeString);
        }
    }
    if(fields & MSGFIELD_process)
        out.append("PID=\"").append(sysInfo.queryProcessID()).append("\" ");
    if(fields & MSGFIELD_thread)
        out.append("TID=\"").append(sysInfo.queryThreadID()).append("\" ");
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            out.append("SessionID=\"unknown\" ");
        else
            out.append("SessionID=\"").append(sysInfo.querySessionID()).append("\" ");
    }
    if(fields & MSGFIELD_node)
    {
        out.append("Node=\"");
        sysInfo.queryNode()->getUrlStr(out);
        out.append("\" ");
    }
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allSysInfo) out.append("\n     ");
#endif
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            out.append("JobID=\"unknown\" ");
        else
            out.append("JobID=\"").append(jobInfo.queryJobID()).append("\" ");
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            out.append("UserID=\"unknown\" ");
        else
            out.append("UserID=\"").append(jobInfo.queryUserID()).append("\" ");
    }
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allJobInfo) out.append("\n     ");
#endif
    if(fields & MSGFIELD_component) out.append("Component=\"").append(component).append("\" ");
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        out.append("code=\"").append(msgCode).append("\" ");
    out.append("text=\"").append(text.str()).append("\" />\n");
    return out;
}

StringBuffer & LogMsg::toStringTable(StringBuffer & out, unsigned fields) const
{
    out.ensureCapacity(LOG_MSG_FORMAT_BUFFER_LENGTH);
    if(fields & MSGFIELD_audience)
        out.append(LogMsgAudienceToFixString(category.queryAudience()));
    if(fields & MSGFIELD_class)
        out.append(LogMsgClassToFixString(category.queryClass()));
    if(fields & MSGFIELD_detail)
        out.appendf("%10d ", category.queryDetail());
    if(fields & MSGFIELD_msgID)
        out.appendf("%8X ", sysInfo.queryMsgID());
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[12];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 12, "%Y-%m-%d ", &timeStruct);
            out.append(timeString);
        }
        if(fields & MSGFIELD_microTime)
        {
            out.appendf("%02d:%02d:%02d.%06d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            out.appendf("%02d:%02d:%02d.%03d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 12, "%H:%M:%S ", &timeStruct);
            out.append(timeString);
        }
    }
    if(fields & MSGFIELD_process)
        out.appendf("%5d ",sysInfo.queryProcessID());
    if(fields & MSGFIELD_thread)
        out.appendf("%5d ",sysInfo.queryThreadID());
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            out.append("      unknown        ");
        else
            out.appendf("%20" I64F "u ", sysInfo.querySessionID());
    }
    if(fields & MSGFIELD_node)
    {
        size32_t len = out.length();
        sysInfo.queryNode()->getUrlStr(out);
        out.appendN(20 + len - out.length(), ' ');
    }
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            out.append("unknown ");
        else
            out.appendf("%7" I64F "u ", jobInfo.queryJobID());
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            out.append("unknown ");
        else
            out.appendf("%7" I64F "u ", jobInfo.queryUserID());
    }
    if(fields & MSGFIELD_component)
        out.appendf("%6u ", component);
    if (fields & MSGFIELD_quote)
        out.append('"');
    if (fields & MSGFIELD_prefix)
        out.append(msgPrefix(category.queryClass()));
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        out.append(msgCode).append(": ").append(text.str());
    else
        out.append(text.str());
    if (fields & MSGFIELD_quote)
        out.append('"');
    out.append('\n');
    return out;
}

StringBuffer & LogMsg::toStringTableHead(StringBuffer & out, unsigned fields)
{
    if(fields & MSGFIELD_audience)
        out.append("Audience ");
    if(fields & MSGFIELD_class)
        out.append("Class    ");
    if(fields & MSGFIELD_detail)
        out.append("    Detail ");
    if(fields & MSGFIELD_msgID)
        out.append("   MsgID ");
    if(fields & MSGFIELD_date)
        out.append("      Date ");
    if(fields & (MSGFIELD_microTime | MSGFIELD_milliTime | MSGFIELD_time))
        out.append("    Time ");
    if(fields & MSGFIELD_process)
        out.append("  PID ");
    if(fields & MSGFIELD_thread)
        out.append("  TID ");
    if(fields & MSGFIELD_session)
        out.append("      SessionID      ");
    if(fields & MSGFIELD_node)
        out.append("               Node ");
    if(fields & MSGFIELD_job)
        out.append("  JobID ");
    if(fields & MSGFIELD_user)
        out.append(" UserID ");
    if(fields & MSGFIELD_component)
        out.append(" Compo ");
    out.append("\n\n");
    return out;
}

void LogMsg::fprintPlain(FILE * handle, unsigned fields) const
{
    if(fields & MSGFIELD_audience)
        fprintf(handle, "aud=%s", LogMsgAudienceToVarString(category.queryAudience()));
    if(fields & MSGFIELD_class)
        fprintf(handle, "cls=%s", LogMsgClassToVarString(category.queryClass()));
    if(fields & MSGFIELD_detail)
        fprintf(handle, "det=%d ", category.queryDetail());
    if(fields & MSGFIELD_msgID)
        fprintf(handle, "id=%X ", sysInfo.queryMsgID());
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[12];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 12, "%Y-%m-%d ", &timeStruct);
            fputs(timeString, handle);
        }
        if(fields & MSGFIELD_microTime)
        {
            fprintf(handle, "%02d:%02d:%02d.%06d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            fprintf(handle, "%02d:%02d:%02d.%03d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 12, "%H:%M:%S ", &timeStruct);
            fputs(timeString, handle);
        }
    }
    if(fields & MSGFIELD_process)
        fprintf(handle, "pid=%d ",sysInfo.queryProcessID());
    if(fields & MSGFIELD_thread)
        fprintf(handle, "tid=%d ",sysInfo.queryThreadID());
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            fprintf(handle, "sid=unknown ");
        else
            fprintf(handle, "sid=%" I64F "u ", sysInfo.querySessionID());
    }
    if(fields & MSGFIELD_node)
    {
        StringBuffer buff;
        sysInfo.queryNode()->getUrlStr(buff);
        fprintf(handle, "%s ", buff.str());
    }
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            fprintf(handle, "job=unknown ");
        else
            fprintf(handle, "job=%" I64F "u ", jobInfo.queryJobID());
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            fprintf(handle, "usr=unknown ");
        else
            fprintf(handle, "usr=%" I64F "u ", jobInfo.queryUserID());
    }
    if(fields & MSGFIELD_component)
        fprintf(handle, "cmp=%u ", component);
    
    const char * quote = (fields & MSGFIELD_quote) ? "\"" : "";
    const char * prefix = (fields & MSGFIELD_prefix) ? msgPrefix(category.queryClass()) : "";
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        fprintf(handle, "%s%s%d: %s%s", quote, prefix, msgCode, text.str(), quote);
    else
        fprintf(handle, "%s%s%s%s", quote, prefix, text.str(), quote);
}

void LogMsg::fprintXML(FILE * handle, unsigned fields) const
{
    fprintf(handle, "<msg ");
    if(fields & MSGFIELD_audience)
        fprintf(handle, "Audience=\"%s\" ", LogMsgAudienceToVarString(category.queryAudience()));
    if(fields & MSGFIELD_class)
        fprintf(handle, "Class=\"%s\" ", LogMsgClassToVarString(category.queryClass()));
    if(fields & MSGFIELD_detail)
        fprintf(handle, "Detail=\"%d\" ", category.queryDetail());
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allCategory) fprintf(handle, "\n     ");
#endif
    if(fields & MSGFIELD_msgID)
        fprintf(handle, "MessageID=\"%d\" ",sysInfo.queryMsgID());
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[20];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 20, "date=\"%Y-%m-%d\" ", &timeStruct);
            fputs(timeString, handle);
        }
        if(fields & MSGFIELD_microTime)
        {
            fprintf(handle, "time=\"%02d:%02d:%02d.%06d\" ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            fprintf(handle, "time=\"%02d:%02d:%02d.%03d\" ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 20, "time=\"%H:%M:%S\" ", &timeStruct);
            fputs(timeString, handle);
        }
    }
    if(fields & MSGFIELD_process)
        fprintf(handle, "PID=\"%d\" ", sysInfo.queryProcessID());
    if(fields & MSGFIELD_thread)
        fprintf(handle, "TID=\"%d\" ", sysInfo.queryThreadID());
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            fprintf(handle, "SessionID=\"unknown\" ");
        else
            fprintf(handle, "SessionID=\"%" I64F "u\" ", sysInfo.querySessionID());
    }
    if(fields & MSGFIELD_node)
    {
        StringBuffer buff;
        sysInfo.queryNode()->getUrlStr(buff);
        fprintf(handle, "Node=\"%s\" ", buff.str());
    }
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allSysInfo) fprintf(handle, "\n     ");
#endif
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            fprintf(handle, "JobID=\"unknown\" ");
        else
            fprintf(handle, "JobID=\"%" I64F "u\" ", jobInfo.queryJobID());
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            fprintf(handle, "UserID=\"unknown\" ");
        else
            fprintf(handle, "UserID=\"%" I64F "u\" ", jobInfo.queryUserID());
    }
    if(fields & MSGFIELD_component)
        fprintf(handle, "Component=\"%6u\" ", component);
#ifdef LOG_MSG_NEWLINE
    if(fields & MSGFIELD_allJobInfo) fprintf(handle, "\n     ");
#endif
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        fprintf(handle, "code=\"%d\" ", msgCode);
    fprintf(handle, "text=\"%s\" />\n", text.str());
}

void LogMsg::fprintTable(FILE * handle, unsigned fields) const
{
    if(fields & MSGFIELD_audience)
        fputs(LogMsgAudienceToFixString(category.queryAudience()), handle);
    if(fields & MSGFIELD_class)
        fputs(LogMsgClassToFixString(category.queryClass()), handle);
    if(fields & MSGFIELD_detail)
        fprintf(handle, "%10d ", category.queryDetail());
    if(fields & MSGFIELD_msgID)
        fprintf(handle, "%08X ", sysInfo.queryMsgID());
    if(fields & MSGFIELD_timeDate)
    {
        time_t timeNum = sysInfo.queryTime();
        char timeString[12];
        struct tm timeStruct;
        localtime_r(&timeNum, &timeStruct);
        if(fields & MSGFIELD_date)
        {
            strftime(timeString, 12, "%Y-%m-%d ", &timeStruct);
            fputs(timeString, handle);
        }
        if(fields & MSGFIELD_microTime)
        {
            fprintf(handle, "%02d:%02d:%02d.%06d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs());
        }
        else if(fields & MSGFIELD_milliTime)
        {
            fprintf(handle, "%02d:%02d:%02d.%03d ", timeStruct.tm_hour, timeStruct.tm_min, timeStruct.tm_sec, sysInfo.queryUSecs()/1000);
        }
        else if(fields & MSGFIELD_time)
        {
            strftime(timeString, 12, "%H:%M:%S ", &timeStruct);
            fputs(timeString, handle);
        }
    }
    if(fields & MSGFIELD_process)
        fprintf(handle, "%5d ",sysInfo.queryProcessID());
    if(fields & MSGFIELD_thread)
        fprintf(handle, "%5d ",sysInfo.queryThreadID());
    if(fields & MSGFIELD_session)
    {
        if(sysInfo.querySessionID() == UnknownSession)
            fprintf(handle, "       unknown       ");
        else
            fprintf(handle, "%20" I64F "u ", sysInfo.querySessionID());
    }
    if(fields & MSGFIELD_node)
    {
        StringBuffer buff;
        static const char * twenty_spaces = "                    ";
        sysInfo.queryNode()->getUrlStr(buff);
        fprintf(handle, "%s%s", buff.str(), (buff.length()<=20) ? twenty_spaces+buff.length() : "");
    }
    if(fields & MSGFIELD_job)
    {
        if(jobInfo.queryJobID() == UnknownJob)
            fprintf(handle, "unknown ");
        else
            fprintf(handle, "%7" I64F "u ", jobInfo.queryJobID());
    }
    if(fields & MSGFIELD_user)
    {
        if(jobInfo.queryUserID() == UnknownUser)
            fprintf(handle, "unknown ");
        else
            fprintf(handle, "%7" I64F "u ", jobInfo.queryUserID());
    }
    if(fields & MSGFIELD_component)
        fprintf(handle, "%6u ", component);
    const char * quote = (fields & MSGFIELD_quote) ? "\"" : "";
    const char * prefix = (fields & MSGFIELD_prefix) ? msgPrefix(category.queryClass()) : "";
    if((fields & MSGFIELD_code) && (msgCode != NoLogMsgCode))
        fprintf(handle, "%s%s%d: %s%s\n", quote, prefix, msgCode, text.str(), quote);
    else
        fprintf(handle, "%s%s%s%s\n", quote, prefix, text.str(), quote);
}

void LogMsg::fprintTableHead(FILE * handle, unsigned fields)
{
    if(fields & MSGFIELD_audience)
        fprintf(handle, "Audience ");
    if(fields & MSGFIELD_class)
        fprintf(handle, "Class    ");
    if(fields & MSGFIELD_detail)
        fprintf(handle, "    Detail ");
    if(fields & MSGFIELD_msgID)
        fprintf(handle, "   MsgID ");
    if(fields & MSGFIELD_date)
        fprintf(handle, "      Date ");
    if(fields & MSGFIELD_time)
        fprintf(handle, "    Time ");
    if(fields & MSGFIELD_process)
        fprintf(handle, "  PID ");
    if(fields & MSGFIELD_thread)
        fprintf(handle, "  TID ");
    if(fields & MSGFIELD_session)
        fprintf(handle, "      SessionID      ");
    if(fields & MSGFIELD_node)
        fprintf(handle, "               Node ");
    if(fields & MSGFIELD_job)
        fprintf(handle, "  JobID ");
    if(fields & MSGFIELD_user)
        fprintf(handle, " UserID ");
    if(fields & MSGFIELD_component)
        fprintf(handle, " Compo ");
    fprintf(handle, "\n\n");
}

// Implementations of ILogMsgFilter

void PassAllLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "all");
    tree->addPropTree("filter", filterTree);
}

void PassLocalLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "local");
    tree->addPropTree("filter", filterTree);
}

void PassNoneLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "none");
    tree->addPropTree("filter", filterTree);
}

void CategoryLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "category");
    filterTree->setPropInt("@audience", audienceMask);
    filterTree->setPropInt("@class", classMask);
    filterTree->setPropInt("@detail", maxDetail);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void PIDLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "pid");
    filterTree->setPropInt("@pid", pid);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void TIDLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "tid");
    filterTree->setPropInt("@tid", tid);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void NodeLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "node");
    StringBuffer buff;
    node.getIpText(buff);
    filterTree->setProp("@ip", buff.str());
    filterTree->setPropInt("@port", node.port);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void IpLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "ip");
    StringBuffer buff;
    ip.getIpText(buff);
    filterTree->setProp("@ip", buff.str());
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void JobLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "job");
    filterTree->setPropInt("@job", (int)job);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void UserLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "user");
    filterTree->setPropInt("@user", (int)user);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void SessionLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "session");
    filterTree->setPropInt("@session", (int)session);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void ComponentLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "component");
    filterTree->setPropInt("@component", component);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

bool RegexLogMsgFilter::includeMessage(const LogMsg & msg) const 
{ 
    if(localFlag && msg.queryRemoteFlag()) return false; 
    SpinBlock b(lock);
    return const_cast<RegExpr &>(regex).find(msg.queryText()) != NULL;
}

void RegexLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "regex");
    filterTree->setProp("@regex", regexText);
    if(localFlag) filterTree->setPropInt("@local", 1);
    tree->addPropTree("filter", filterTree);
}

void NotLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "not");
    arg->addToPTree(filterTree);
    tree->addPropTree("filter", filterTree);
}

void AndLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "and");
    arg1->addToPTree(filterTree);
    arg2->addToPTree(filterTree);
    tree->addPropTree("filter", filterTree);
}

void OrLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "or");
    arg1->addToPTree(filterTree);
    arg2->addToPTree(filterTree);
    tree->addPropTree("filter", filterTree);
}

void SwitchLogMsgFilter::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * filterTree = createPTree(ipt_caseInsensitive);
    filterTree->setProp("@type", "switch");
    cond->addToPTree(filterTree);
    yes->addToPTree(filterTree);
    no->addToPTree(filterTree);
    tree->addPropTree("filter", filterTree);
}

void CategoryLogMsgFilter::orWithFilter(const ILogMsgFilter * filter)
{
    audienceMask |= filter->queryAudienceMask();
    classMask |= filter->queryClassMask();
    maxDetail = std::max(maxDetail, filter->queryMaxDetail());
}

void CategoryLogMsgFilter::reset()
{
    audienceMask = 0;
    classMask = 0;
    maxDetail = 0;
}

// HandleLogMsgHandler

void HandleLogMsgHandlerTable::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    if(handle==stderr)
        handlerTree->setProp("@type", "stderr");
    else
        handlerTree->setProp("@type", "mischandle");
    handlerTree->setPropInt("@fields", messageFields);
    tree->addPropTree("handler", handlerTree);
}

void HandleLogMsgHandlerXML::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    if(handle==stderr)
        handlerTree->setProp("@type", "stderr");
    else
        handlerTree->setProp("@type", "mischandle");
    handlerTree->setPropInt("@fields", messageFields);
    handlerTree->setProp("@writeXML", "true");
    tree->addPropTree("handler", handlerTree);
}

// FileLogMsgHandler

FileLogMsgHandler::FileLogMsgHandler(const char * _filename, const char * _headerText, unsigned _fields, bool _append, bool _flushes)
   : messageFields(_fields), filename(_filename), headerText(_headerText), append(_append), flushes(_flushes)
{
    recursiveCreateDirectoryForFile(filename);
    if(append)
        handle = fopen(filename, "a");
    else
        handle = fopen(filename, "w");
    if(!handle) {
        handle = getNullHandle();
        StringBuffer err;
        err.appendf("LOGGING: could not open file '%s' for output",filename.get());
        OERRLOG("%s",err.str()); // make sure doesn't get lost!
        throw MakeStringException(3000,"%s",err.str()); // 3000: internal error
    }
    if(headerText) fprintf(handle, "--- %s ---\n", (const char *)headerText);
}

static void closeAndDeleteEmpty(const char * filename, FILE *handle)
{
    if (handle) {
        fpos_t pos;
        bool del = (fgetpos(handle, &pos)==0)&&
#if defined( _WIN32) || defined(__FreeBSD__) || defined(__APPLE__)
            (pos==0);
#else
            (pos.__pos==0);
#endif
        fclose(handle);
        if (del)
            remove(filename);
    }
}

FileLogMsgHandler::~FileLogMsgHandler()
{
    closeAndDeleteEmpty(filename,handle);
}

char const * FileLogMsgHandler::disable()
{
    crit.enter();
    fclose(handle);
    handle = NULL;
    return filename;
}

void FileLogMsgHandler::enable()
{
    recursiveCreateDirectoryForFile(filename);
    handle = fopen(filename, "a");
    if(!handle) {
        handle = getNullHandle();
        assertex(!"FileLogMsgHandler::enable : could not open file for output");
    }
    crit.leave();
}

void FileLogMsgHandlerTable::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    handlerTree->setProp("@type", "file");
    handlerTree->setProp("@filename", filename.get());
    if(headerText) handlerTree->setProp("@headertext", headerText.get());
    handlerTree->setPropInt("@fields", messageFields);
    handlerTree->setProp("@writeTable", "true");
    if(append) handlerTree->setProp("@append", "true");
    if(flushes) handlerTree->setProp("@flushes", "true");
    tree->addPropTree("handler", handlerTree);
}

void FileLogMsgHandlerXML::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    handlerTree->setProp("@type", "file");
    handlerTree->setProp("@filename", filename.get());
    if(headerText) handlerTree->setProp("@headertext", headerText.get());
    handlerTree->setPropInt("@fields", messageFields);
    if(append) handlerTree->setProp("@append", "true");
    if(flushes) handlerTree->setProp("@flushes", "true");
    tree->addPropTree("handler", handlerTree);
}

// RollingFileLogMsgHandler

RollingFileLogMsgHandler::RollingFileLogMsgHandler(const char * _filebase, const char * _fileextn, unsigned _fields, bool _append, bool _flushes, const char *initialName, const char *_alias, bool daily)
  : handle(0), messageFields(_fields), alias(_alias), filebase(_filebase), fileextn(_fileextn), append(_append), flushes(_flushes)
{
    time_t tNow;
    time(&tNow);
    localtime_r(&tNow, &startTime);
    doRollover(daily, initialName);
}

RollingFileLogMsgHandler::~RollingFileLogMsgHandler()
{
    closeAndDeleteEmpty(filename,handle);
}

char const * RollingFileLogMsgHandler::disable()
{
    crit.enter();
    fclose(handle);
    return filename;
}

void RollingFileLogMsgHandler::enable()
{
    recursiveCreateDirectoryForFile(filename);
    handle = fopen(filename, "a");
    if(!handle) {
        handle = getNullHandle();
        assertex(!"RollingFileLogMsgHandler::enable : could not open file for output");
    }
    crit.leave();
}

void RollingFileLogMsgHandler::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    handlerTree->setProp("@type", "rollingfile");
    handlerTree->setProp("@filebase", filebase.get());
    handlerTree->setProp("@fileextn", fileextn.get());
    handlerTree->setPropInt("@fields", messageFields);
    if(append) handlerTree->setProp("@append", "true");
    if(flushes) handlerTree->setProp("@flushes", "true");
    tree->addPropTree("handler", handlerTree);
}

#define ROLLOVER_PERIOD 86400

void RollingFileLogMsgHandler::checkRollover() const
{
    time_t tNow;
    time(&tNow);
    struct tm ltNow;
    localtime_r(&tNow, &ltNow);
    if(ltNow.tm_year != startTime.tm_year || ltNow.tm_yday != startTime.tm_yday)
    {
        localtime_r(&tNow, &startTime);  // reset the start time for next rollover check
        doRollover(true);
    }
}

void RollingFileLogMsgHandler::doRollover(bool daily, const char *forceName) const
{
    CriticalBlock block(crit);
    closeAndDeleteEmpty(filename,handle);
    handle = 0;
    filename.clear();
    if (forceName)
        filename.append(forceName);
    else
    {
        filename.clear().append(filebase.get());
        addFileTimestamp(filename, daily);
        filename.append(fileextn.get());
    }
    recursiveCreateDirectoryForFile(filename.str());
    handle = fopen(filename.str(), append ? "a" : "w");
    if (handle && alias && alias.length())
    {
        fclose(handle);
        handle = 0;
        remove(alias);
        try
        {
            createHardLink(alias, filename.str());
        }
        catch (IException *E)
        {
            recursiveCreateDirectoryForFile(filename.str());
            handle = fopen(filename.str(), append ? "a" : "w");
            EXCLOG(E);  // Log the fact that we could not create the alias - probably it is locked (tail a bit unfortunate on windows).
            E->Release();
        }
        if (!handle)
        {
            recursiveCreateDirectoryForFile(filename.str());
            handle = fopen(filename.str(), append ? "a" : "w");
        }
    }
    if(!handle) 
    {
        handle = getNullHandle();
        OWARNLOG("RollingFileLogMsgHandler::doRollover : could not open log file %s for output", filename.str());
        // actually this is pretty fatal
    }
}

// BinLogMsgHandler

BinLogMsgHandler::BinLogMsgHandler(const char * _filename, bool _append) : filename(_filename), append(_append)
{
    file.setown(createIFile(filename.get()));
    if(!file) assertex(!"BinLogMsgHandler::BinLogMsgHandler : Could not create IFile");
    if(append)
        fio.setown(file->open(IFOwrite));
    else
        fio.setown(file->open(IFOcreate));
    if(!fio) assertex(!"BinLogMsgHandler::BinLogMsgHandler : Could not create IFileIO");
    fstr.setown(createIOStream(fio));
    if(!fstr) assertex(!"BinLogMsgHandler::BinLogMsgHandler : Could not create IFileIOStream");
    if(append)
        fstr->seek(0, IFSend);
}

BinLogMsgHandler::~BinLogMsgHandler()
{
    fstr.clear();
    fio.clear();
    file.clear();
}

void BinLogMsgHandler::handleMessage(const LogMsg & msg) const
{
    CriticalBlock block(crit);
    mbuff.clear();
    msg.serialize(mbuff);
    msglen = mbuff.length();
    fstr->write(sizeof(msglen), &msglen);
    fstr->write(msglen, mbuff.toByteArray());
}

void BinLogMsgHandler::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    handlerTree->setProp("@type", "binary");
    handlerTree->setProp("@filename", filename.get());
    if(append) handlerTree->setProp("@append", "true");
    tree->addPropTree("handler", handlerTree);
}

char const * BinLogMsgHandler::disable()
{
    crit.enter();
    fstr.clear();
    fio.clear();
    return filename.get();
}

void BinLogMsgHandler::enable()
{
    fio.setown(file->open(IFOwrite));
    if(!fio) assertex(!"BinLogMsgHandler::enable : Could not create IFileIO");
    fstr.setown(createIOStream(fio));
    if(!fstr) assertex(!"BinLogMsgHandler::enable : Could not create IFileIOStream");
    fstr->seek(0, IFSend);
    crit.leave();
}

// LogMsgComponentReporter

void LogMsgComponentReporter::report(const LogMsgCategory & cat, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    queryLogMsgManager()->report_va(component, cat, unknownJob, format, args);
    va_end(args);
}

void LogMsgComponentReporter::report_va(const LogMsgCategory & cat, const char * format, va_list args)
{
    queryLogMsgManager()->report_va(component, cat, unknownJob, format, args);
}

void LogMsgComponentReporter::report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    queryLogMsgManager()->report_va(component, cat, unknownJob, code, format, args);
    va_end(args);
}

void LogMsgComponentReporter::report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args)
{
    queryLogMsgManager()->report_va(component, cat, unknownJob, code, format, args);
}

void LogMsgComponentReporter::report(const LogMsgCategory & cat, const IException * exception, const char * prefix)
{
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    queryLogMsgManager()->report(component, cat, unknownJob, exception->errorCode(), "%s", buff.str());
}

void LogMsgComponentReporter::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    queryLogMsgManager()->report_va(component, cat, job, format, args);
    va_end(args);
}

void LogMsgComponentReporter::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args)
{
    queryLogMsgManager()->report_va(component, cat, job, format, args);
}

void LogMsgComponentReporter::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...)
{
    va_list args;
    va_start(args, format);
    queryLogMsgManager()->report_va(component, cat, job, code, format, args);
    va_end(args);
}

void LogMsgComponentReporter::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args)
{
    queryLogMsgManager()->report_va(component, cat, job, code, format, args);
}

void LogMsgComponentReporter::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * exception, const char * prefix)
{
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    queryLogMsgManager()->report(component, cat, job, exception->errorCode(), "%s", buff.str());
}

void LogMsgComponentReporter::report(const LogMsg & msg)
{
    queryLogMsgManager()->report(msg);
}

// LogMsgPrepender

void LogMsgPrepender::report(const LogMsgCategory & cat, const char * format, ...)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    va_list args;
    va_start(args, format);
    if(reporter)
        reporter->report_va(cat, unknownJob, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, unknownJob, buff.str(), args);
    va_end(args);
}

void LogMsgPrepender::report_va(const LogMsgCategory & cat, const char * format, va_list args)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    if(reporter)
        reporter->report_va(cat, unknownJob, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, unknownJob, buff.str(), args);
}

void LogMsgPrepender::report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    va_list args;
    va_start(args, format);
    if(reporter)
        reporter->report_va(cat, unknownJob, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, unknownJob, buff.str(), args);
    va_end(args);
}

void LogMsgPrepender::report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    if(reporter)
        reporter->report_va(cat, unknownJob, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, unknownJob, buff.str(), args);
}

void LogMsgPrepender::report(const LogMsgCategory & cat, const IException * exception, const char * prefix)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ");
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    if(reporter)
        reporter->report(cat, unknownJob, exception->errorCode(), "%s",  buff.str());
    else
        queryLogMsgManager()->report(cat, unknownJob, exception->errorCode(), "%s", buff.str());
}

void LogMsgPrepender::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    va_list args;
    va_start(args, format);
    if(reporter)
        reporter->report_va(cat, job, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, job, buff.str(), args);
    va_end(args);
}

void LogMsgPrepender::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    if(reporter)
        reporter->report_va(cat, job, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, job, buff.str(), args);
}

void LogMsgPrepender::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    va_list args;
    va_start(args, format);
    if(reporter)
        reporter->report_va(cat, job, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, job, buff.str(), args);
    va_end(args);
}

void LogMsgPrepender::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args)
{
    StringBuffer buff;
    buff.append(file).append("(").append(line).append(") : ").append(format);
    if(reporter)
        reporter->report_va(cat, job, buff.str(), args);
    else
        queryLogMsgManager()->report_va(cat, job, buff.str(), args);
}

void LogMsgPrepender::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * exception, const char * prefix)
{
    StringBuffer txt;
    if (prefix) 
        txt.append(prefix).append(" : ");
    exception->errorMessage(txt);
    if (reporter)
        reporter->report(cat, job, exception->errorCode(), "%s(%d) : %s", file, line, txt.str());
    else
        queryLogMsgManager()->report(cat, job, exception->errorCode(), "%s(%d) : %s", file, line, txt.str());
}

IException * LogMsgPrepender::report(IException * e, const char * prefix, LogMsgClass cls)
{
    report(MCexception(e, cls), unknownJob, e, prefix);
    return e;
}

// LogMsgMonitor

void LogMsgMonitor::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * monitorTree = createPTree(ipt_caseInsensitive);
    handler->addToPTree(monitorTree);
    filter->addToPTree(monitorTree);
    tree->addPropTree("monitor", monitorTree);
}

// CLogMsgManager

void CLogMsgManager::MsgProcessor::push(LogMsg * msg)
{
    //assertex(more); an assertex will just recurse here
    if (!more) // we are effective stopped so don't bother even dropping (and leak parameter) as drop will involve 
               // interaction with the base class which is stopped and could easily crash (as this condition 
               // is expected not to occur - typically occurs if the user has incorrectly called exit on one thread 
               // while still in the process of logging on another)
               // cf Bug #53695 for more discussion of the issue
        return;
    else if(droppingLimit && (q.ordinality() >= droppingLimit))
        drop();
    q.enqueue(msg);
}

int CLogMsgManager::MsgProcessor::run()
{
    Owned<LogMsg> msg;
    while(more)
    {
        msg.setown(q.dequeueAndNotify(this)); // notify locks mutex on non-null return
        if(!msg)
            break;
        owner->doReport(*msg);
        pullCycleMutex.unlock();
    }
    while(true)
    {
        msg.setown(q.dequeueNowAndNotify(this)); // notify locks mutex on non-null return
        if(!msg)
            break;
        owner->doReport(*msg);
        pullCycleMutex.unlock();
    }
    return 0;
}

void CLogMsgManager::MsgProcessor::notify(LogMsg *)
{
    pullCycleMutex.lock();
}

void CLogMsgManager::MsgProcessor::setBlockingLimit(unsigned lim)
{
    q.setLimit(lim);
    droppingLimit = 0;
}

void CLogMsgManager::MsgProcessor::setDroppingLimit(unsigned lim, unsigned num)
{
    numToDrop = num;
    droppingLimit = lim;
    q.setLimit(0);
}

void CLogMsgManager::MsgProcessor::resetLimit()
{
    droppingLimit = 0;
    q.setLimit(0);
}

void CLogMsgManager::MsgProcessor::stop()
{
    more = false;
    q.stop();
}

void CLogMsgManager::MsgProcessor::drop()
{
    Owned<LogMsg> msg, lastMsg;
    unsigned count;
    unsigned prev = 0;
    for(count = 0; count < numToDrop; count++)
    {
        msg.setown(q.dequeueTail(0));
        if(!msg) break;
        DropLogMsg * dmsg = dynamic_cast<DropLogMsg *>(msg.get());
        if(dmsg) prev += dmsg->queryCount()-1;
        lastMsg.setown(msg.getClear());
    }
    if(lastMsg)
        q.enqueue(new DropLogMsg(owner, lastMsg->querySysInfo().queryMsgID(), count+prev));
}

bool CLogMsgManager::MsgProcessor::flush(unsigned timeout)
{
    unsigned start = msTick();
    if(!q.waitMaxOrdinality(0, timeout))
        return false;
    unsigned now = msTick();
    if(now >= (start+timeout))
        return false;
    try
    {
        synchronized block(pullCycleMutex, timeout+start-now);
    }
    catch(IException * e)
    {
        e->Release();
        return false;
    }
    return true;
}

CLogMsgManager::~CLogMsgManager()
{
    CriticalBlock crit(modeLock);
    if(processor)
    {
        processor->stop();
        processor->join();
    }
}

void CLogMsgManager::enterQueueingMode()
{
    CriticalBlock crit(modeLock);
    if(processor) return;
    processor.setown(new MsgProcessor(this));
    processor->setBlockingLimit(defaultMsgQueueLimit);
    processor->start();
}

void CLogMsgManager::setQueueBlockingLimit(unsigned lim)
{
    CriticalBlock crit(modeLock); 
    if(processor)
        processor->setBlockingLimit(lim);
}

void CLogMsgManager::setQueueDroppingLimit(unsigned lim, unsigned numToDrop)
{
    CriticalBlock crit(modeLock); 
    if(processor)
        processor->setDroppingLimit(lim, numToDrop);
}

void CLogMsgManager::resetQueueLimit()
{
    CriticalBlock crit(modeLock);
    if(processor)
        processor->resetLimit();
}

void CLogMsgManager::report(const LogMsgCategory & cat, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, NoLogMsgCode, format, args, 0, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(const LogMsgCategory & cat, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, NoLogMsgCode, format, args, 0, port, session));
}

void CLogMsgManager::report(const LogMsgCategory & cat, LogMsgCode code, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, code, format, args, 0, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, code, format, args, 0, port, session));
}

void CLogMsgManager::report(const LogMsgCategory & cat, const IException * exception, const char * prefix)
{
    if(rejectsCategory(cat)) return;
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, exception->errorCode(), buff.str(), 0, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, NoLogMsgCode, format, args, compo, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(unsigned compo, const LogMsgCategory & cat, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, NoLogMsgCode, format, args, compo, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, LogMsgCode code, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, code, format, args, compo, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(unsigned compo, const LogMsgCategory & cat, LogMsgCode code, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, code, format, args, compo, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, const IException * exception, const char * prefix)
{
    if(rejectsCategory(cat)) return;
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    pushMsg(new LogMsg(cat, getNextID(), unknownJob, exception->errorCode(), buff.str(), compo, port, session));
}

void CLogMsgManager::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), job, NoLogMsgCode, format, args, 0, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), job, NoLogMsgCode, format, args, 0, port, session));
}

void CLogMsgManager::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), job, code, format, args, 0, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), job, code, format, args, 0, port, session));
}

void CLogMsgManager::report(const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * exception, const char * prefix)
{
    if(rejectsCategory(cat)) return;
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    pushMsg(new LogMsg(cat, getNextID(), job, exception->errorCode(), buff.str(), 0, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), job, NoLogMsgCode, format, args, compo, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), job, NoLogMsgCode, format, args, compo, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, ...)
{
    if(rejectsCategory(cat)) return;
    va_list args;
    va_start(args, format);
    pushMsg(new LogMsg(cat, getNextID(), job, code, format, args, compo, port, session));
    va_end(args);
}

void CLogMsgManager::report_va(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, LogMsgCode code, const char * format, va_list args)
{
    if(rejectsCategory(cat)) return;
    pushMsg(new LogMsg(cat, getNextID(), job, code, format, args, compo, port, session));
}

void CLogMsgManager::report(unsigned compo, const LogMsgCategory & cat, const LogMsgJobInfo & job, const IException * exception, const char * prefix)
{
    if(rejectsCategory(cat)) return;
    StringBuffer buff;
    if(prefix) buff.append(prefix).append(" : ");
    exception->errorMessage(buff);
    pushMsg(new LogMsg(cat, getNextID(), job, exception->errorCode(), buff.str(), compo, port, session));
}

void CLogMsgManager::pushMsg(LogMsg * _msg)
{
    Owned<LogMsg> msg(_msg);
    if(processor)
        processor->push(msg.getLink());
    else
        doReport(*msg);
}

void CLogMsgManager::doReport(const LogMsg & msg) const
{
    try
    {
        ReadLockBlock block(monitorLock);
        ForEachItemIn(i, monitors)
            monitors.item(i).processMessage(msg);
    }
    catch(IException * e)
    {
        StringBuffer err("exception reporting log message: ");
        err.append(e->errorCode());
        e->errorMessage(err);
        panic(err.str());
        e->Release();
    }
    catch(...)
    {
        panic("unknown exception reporting log message");
    }
}

void CLogMsgManager::panic(char const * reason) const
{
    fprintf(stderr, "%s", reason); // not sure there's anything more useful we can do here
}

offset_t CLogMsgManager::getLogPosition(StringBuffer &logFileName, const ILogMsgHandler * handler) const
{
    if (processor)
        processor->flush(10*1000);
    WriteLockBlock block(monitorLock);  // Prevents any incoming messages as we are doing this
    return handler->getLogPosition(logFileName);
}

aindex_t CLogMsgManager::find(const ILogMsgHandler * handler) const
{
    // N.B. Should be used inside critical block
    ForEachItemIn(i, monitors)
        if(monitors.item(i).queryHandler()==handler) return i;
    return NotFound;
}

bool CLogMsgManager::addMonitor(ILogMsgHandler * handler, ILogMsgFilter * filter)
{
    flushQueue(10*1000);
    WriteLockBlock block(monitorLock);
    if(find(handler) != NotFound) return false;
    monitors.append(*(new LogMsgMonitor(filter, handler)));
    prefilter.orWithFilter(filter);
    sendFilterToChildren(true);
    return true;
}

bool CLogMsgManager::addMonitorOwn(ILogMsgHandler * handler, ILogMsgFilter * filter)
{
    bool ret = addMonitor(handler, filter);
    filter->Release();
    handler->Release();
    return ret;
}

void CLogMsgManager::buildPrefilter()
{
    // N.B. Should be used inside critical block
    prefilter.reset();
    ForEachItemIn(i, monitors)
        prefilter.orWithFilter(monitors.item(i).queryFilter());
}

bool CLogMsgManager::removeMonitor(ILogMsgHandler * handler)
{
    Linked<LogMsgMonitor> todelete;
    {
        WriteLockBlock block(monitorLock);
        aindex_t pos = find(handler);
        if(pos == NotFound) return false;
        todelete.set(&monitors.item(pos));
        monitors.remove(pos);
        buildPrefilter();
        sendFilterToChildren(true);
        return true;
    }
}

unsigned CLogMsgManager::removeMonitorsMatching(HandlerTest & test)
{
    CIArrayOf<LogMsgMonitor>  todelete; // delete outside monitorLock
    unsigned count = 0;
    {
        WriteLockBlock block(monitorLock);
        ForEachItemInRev(i, monitors)
            if(test(monitors.item(i).queryHandler()))
            {
                LogMsgMonitor &it = monitors.item(i);
                it.Link();
                todelete.append(it);
                monitors.remove(i);
                ++count;
            }
        buildPrefilter();
        sendFilterToChildren(true);
    }
    return count;
}

void CLogMsgManager::removeAllMonitors()
{
    CIArrayOf<LogMsgMonitor>  todelete; // delete outside monitorLock
    {
        WriteLockBlock block(monitorLock);
        ForEachItemInRev(i, monitors) {
            LogMsgMonitor &it = monitors.item(i);
            it.Link();
            todelete.append(it);
            monitors.remove(i);
        }
        prefilter.reset();
        sendFilterToChildren(true);
    }
}

void CLogMsgManager::resetMonitors()
{
    suspendChildren();
    removeAllMonitors();
    Owned<ILogMsgFilter> defaultFilter = getDefaultLogMsgFilter();
    addMonitor(theStderrHandler, defaultFilter);
    unsuspendChildren();
}

ILogMsgFilter * CLogMsgManager::queryMonitorFilter(const ILogMsgHandler * handler) const
{
    ReadLockBlock block(monitorLock);
    aindex_t pos = find(handler);
    if(pos == NotFound) return 0;
    return monitors.item(pos).queryFilter();
}

bool CLogMsgManager::changeMonitorFilter(const ILogMsgHandler * handler, ILogMsgFilter * newFilter)
{
    WriteLockBlock block(monitorLock);
    aindex_t pos = find(handler);
    if(pos == NotFound) return 0;
    monitors.item(pos).setFilter(newFilter);
    buildPrefilter();
    sendFilterToChildren(true);
    return true;
}

void CLogMsgManager::prepAllHandlers() const
{
    ReadLockBlock block(monitorLock);
    ForEachItemIn(i, monitors)
        if(monitors.item(i).queryHandler()->needsPrep()) monitors.item(i).queryHandler()->prep();
}

aindex_t CLogMsgManager::findChild(ILogMsgLinkToChild * child) const
{
    ForEachItemIn(i, children)
        if(&(children.item(i)) == child ) return i;
    return NotFound;
}

ILogMsgFilter * CLogMsgManager::getCompoundFilter(bool locked) const
{
    if(!locked) monitorLock.lockRead();
    Owned<CategoryLogMsgFilter> categoryFilter = new CategoryLogMsgFilter(0, 0, 0, false);
    Owned<ILogMsgFilter> otherFilters;
    ILogMsgFilter * ifilter;
    bool hadCat = false;
    ForEachItemIn(i, monitors)
    {
        ifilter = monitors.item(i).queryFilter();
        if(ifilter->queryLocalFlag()) continue;
        if(ifilter->isCategoryFilter())
        {
            categoryFilter->orWithFilter(ifilter);
            hadCat = true;
        }
        else
        {
            if(otherFilters)
                otherFilters.setown(getOrLogMsgFilter(otherFilters, ifilter));
            else
                otherFilters.set(ifilter);
        }
    }
    if(hadCat)
    {
        if(otherFilters)
            otherFilters.setown(getOrLogMsgFilter(otherFilters, categoryFilter));
        else
            otherFilters.set(categoryFilter);
    }
    if(!locked) monitorLock.unlock();
    if(!otherFilters)
        return getPassNoneLogMsgFilter();
    return otherFilters.getLink();
}

void CLogMsgManager::sendFilterToChildren(bool locked) const
{
    if(suspendedChildren) return;
    ReadLockBlock block(childLock);
    if(children.length()==0) return;
    ILogMsgFilter * filter = getCompoundFilter(locked);
    ForEachItemIn(i, children)
        children.item(i).sendFilter(filter);
    filter->Release();
}

bool CLogMsgManager::addMonitorToPTree(const ILogMsgHandler * handler, IPropertyTree * tree) const
{
    ReadLockBlock block(monitorLock);
    aindex_t pos = find(handler);
    if(pos == NotFound) return false;
    monitors.item(pos).addToPTree(tree);
    return true;
}

void CLogMsgManager::addAllMonitorsToPTree(IPropertyTree * tree) const
{
    ReadLockBlock block(monitorLock);
    ForEachItemIn(i, monitors)
        monitors.item(i).addToPTree(tree);
}

bool CLogMsgManager::rejectsCategory(const LogMsgCategory & cat) const 
{ 
    if (!prefilter.includeCategory(cat))
        return true;

    ReadLockBlock block(monitorLock);
    ForEachItemIn(i, monitors)
    {
        if (monitors.item(i).queryFilter()->mayIncludeCategory(cat))
            return false;
    }
    return true;
}

// Helper functions

ILogMsgFilter * getDeserializedLogMsgFilter(MemoryBuffer & in)
{
    unsigned type;
    in.read(type);
    switch(type)
    {
    case MSGFILTER_passall : return LINK(thePassAllFilter);
    case MSGFILTER_passlocal : return LINK(thePassLocalFilter);
    case MSGFILTER_passnone : return LINK(thePassNoneFilter);
    case MSGFILTER_category : return new CategoryLogMsgFilter(in);
    case MSGFILTER_pid : return new PIDLogMsgFilter(in);
    case MSGFILTER_tid : return new TIDLogMsgFilter(in);
    case MSGFILTER_node : return new NodeLogMsgFilter(in);
    case MSGFILTER_ip : return new IpLogMsgFilter(in);
    case MSGFILTER_job : return new JobLogMsgFilter(in);
    case MSGFILTER_user : return new UserLogMsgFilter(in);
    case MSGFILTER_session : return new SessionLogMsgFilter(in);
    case MSGFILTER_component : return new ComponentLogMsgFilter(in);
    case MSGFILTER_regex : return new RegexLogMsgFilter(in);
    case MSGFILTER_not : return new NotLogMsgFilter(in);
    case MSGFILTER_and : return new AndLogMsgFilter(in);
    case MSGFILTER_or : return new OrLogMsgFilter(in);
    case MSGFILTER_switch : return new SwitchLogMsgFilter(in);
    default: assertex(!"getDeserializedLogMsgFilter: unrecognized LogMsgFilterType");
    }
    return 0;
}

ILogMsgFilter * getLogMsgFilterFromPTree(IPropertyTree * xml)
{
    /* Note that several of these constructors use GetPropInt and GetPropInt64 to get unsigneds. I think this is OK? (all int64 internally)*/
    StringBuffer type;
    xml->getProp("@type", type);
    if(strcmp(type.str(), "all")==0) return LINK(thePassAllFilter);
    else if(strcmp(type.str(), "local")==0) return LINK(thePassLocalFilter);
    else if(strcmp(type.str(), "none")==0) return LINK(thePassNoneFilter);
    else if(strcmp(type.str(), "category")==0) return new CategoryLogMsgFilter(xml);
    else if(strcmp(type.str(), "pid")==0) return new PIDLogMsgFilter(xml);
    else if(strcmp(type.str(), "tid")==0) return new TIDLogMsgFilter(xml);
    else if(strcmp(type.str(), "node")==0) return new NodeLogMsgFilter(xml);
    else if(strcmp(type.str(), "ip")==0) return new IpLogMsgFilter(xml);
    else if(strcmp(type.str(), "job")==0) return new JobLogMsgFilter(xml);
    else if(strcmp(type.str(), "user")==0) return new UserLogMsgFilter(xml);
    else if(strcmp(type.str(), "session")==0) return new SessionLogMsgFilter(xml);
    else if(strcmp(type.str(), "component")==0) return new ComponentLogMsgFilter(xml);
    else if(strcmp(type.str(), "regex")==0) return new RegexLogMsgFilter(xml);
    else if(strcmp(type.str(), "not")==0) return new NotLogMsgFilter(xml);
    else if(strcmp(type.str(), "and")==0) return new AndLogMsgFilter(xml);
    else if(strcmp(type.str(), "or")==0) return new OrLogMsgFilter(xml);
    else if(strcmp(type.str(), "filter")==0) return new SwitchLogMsgFilter(xml);
    else assertex(!"getLogMsgFilterFromPTree : unrecognized LogMsgFilter type");
    return getPassAllLogMsgFilter();
}

ILogMsgFilter * getDefaultLogMsgFilter()
{
    return new CategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, DefaultDetail, true);
}

ILogMsgFilter * getPassAllLogMsgFilter()
{
    return LINK(thePassAllFilter);
}

ILogMsgFilter * getLocalLogMsgFilter()
{
    return LINK(thePassLocalFilter);
}

ILogMsgFilter * getPassNoneLogMsgFilter()
{
    return LINK(thePassNoneFilter);
}

ILogMsgFilter * queryPassAllLogMsgFilter()
{
    return thePassAllFilter;
}

ILogMsgFilter * queryLocalLogMsgFilter()
{
    return thePassLocalFilter;
}

ILogMsgFilter * queryPassNoneLogMsgFilter()
{
    return thePassNoneFilter;
}

ILogMsgFilter * getCategoryLogMsgFilter(unsigned audiences, unsigned classes, LogMsgDetail maxDetail, bool local)
{
    if((audiences==MSGAUD_all) && (classes==MSGCLS_all) && (maxDetail==TopDetail))
    {
        if(local)
            return LINK(thePassLocalFilter);
        else
            return LINK(thePassAllFilter);
    }
    return new CategoryLogMsgFilter(audiences, classes, maxDetail, local);
}

ILogMsgFilter * getPIDLogMsgFilter(unsigned pid, bool local)
{
    return new PIDLogMsgFilter(pid, local);
}

ILogMsgFilter * getTIDLogMsgFilter(unsigned tid, bool local)
{
    return new TIDLogMsgFilter(tid, local);
}

ILogMsgFilter * getNodeLogMsgFilter(const char * name, unsigned port, bool local)
{
    return new NodeLogMsgFilter(name, port, local);
}

ILogMsgFilter * getNodeLogMsgFilter(const IpAddress & ip, unsigned port, bool local)
{
    return new NodeLogMsgFilter(ip, port, local);
}

ILogMsgFilter * getNodeLogMsgFilter(unsigned port, bool local)
{
    return new NodeLogMsgFilter(port, local);
}

ILogMsgFilter * getIpLogMsgFilter(const char * name, bool local)
{
    return new IpLogMsgFilter(name, local);
}

ILogMsgFilter * getIpLogMsgFilter(const IpAddress & ip, bool local)
{
    return new IpLogMsgFilter(ip, local);
}

ILogMsgFilter * getIpLogMsgFilter(bool local)
{
    return new IpLogMsgFilter(local);
}

ILogMsgFilter * getJobLogMsgFilter(LogMsgJobId job, bool local)
{
    return new JobLogMsgFilter(job, local);
}

ILogMsgFilter * getUserLogMsgFilter(LogMsgUserId user, bool local)
{
    return new UserLogMsgFilter(user, local);
}

ILogMsgFilter * getSessionLogMsgFilter(LogMsgSessionId session, bool local)
{
    return new SessionLogMsgFilter(session, local);
}

ILogMsgFilter * getComponentLogMsgFilter(unsigned component, bool local)
{
    return new ComponentLogMsgFilter(component, local);
}

ILogMsgFilter * getRegexLogMsgFilter(const char *regex, bool local)
{
    return new RegexLogMsgFilter(regex, local);
}

ILogMsgFilter * getNotLogMsgFilter(ILogMsgFilter * arg)
{
    return new NotLogMsgFilter(arg);
}

ILogMsgFilter * getNotLogMsgFilterOwn(ILogMsgFilter * arg)
{
    ILogMsgFilter * ret = new NotLogMsgFilter(arg);
    arg->Release();
    return ret;
}

ILogMsgFilter * getAndLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2)
{
    return new AndLogMsgFilter(arg1, arg2);
}

ILogMsgFilter * getAndLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2)
{
    ILogMsgFilter * ret = new AndLogMsgFilter(arg1, arg2);
    arg1->Release();
    arg2->Release();
    return ret;
}

ILogMsgFilter * getOrLogMsgFilter(ILogMsgFilter * arg1, ILogMsgFilter * arg2)
{
    return new OrLogMsgFilter(arg1, arg2);
}

ILogMsgFilter * getOrLogMsgFilterOwn(ILogMsgFilter * arg1, ILogMsgFilter * arg2)
{
    ILogMsgFilter * ret = new OrLogMsgFilter(arg1, arg2);
    arg1->Release();
    arg2->Release();
    return ret;
}

ILogMsgFilter * getSwitchLogMsgFilterOwn(ILogMsgFilter * switchFilter, ILogMsgFilter * yesFilter, ILogMsgFilter * noFilter)
{
    ILogMsgFilter * ret = new SwitchLogMsgFilter(switchFilter, yesFilter, noFilter);
    switchFilter->Release();
    yesFilter->Release();
    noFilter->Release();
    return ret;
}

ILogMsgHandler * getHandleLogMsgHandler(FILE * handle, unsigned fields, bool writeXML)
{
    if(writeXML)
        return new HandleLogMsgHandlerXML(handle, fields);
    return new HandleLogMsgHandlerTable(handle, fields);
}

ILogMsgHandler * getFileLogMsgHandler(const char * filename, const char * headertext, unsigned fields, bool writeXML, bool append, bool flushes)
{
    if(writeXML)
        return new FileLogMsgHandlerXML(filename, headertext, fields, append, flushes);
    return new FileLogMsgHandlerTable(filename, headertext, fields, append, flushes);
}

ILogMsgHandler * getRollingFileLogMsgHandler(const char * filebase, const char * fileextn, unsigned fields, bool append, bool flushes, const char *initialName, const char *alias, bool daily)
{
    return new RollingFileLogMsgHandler(filebase, fileextn, fields, append, flushes, initialName, alias, daily);
}

ILogMsgHandler * getBinLogMsgHandler(const char * filename, bool append)
{
    return new BinLogMsgHandler(filename, append);
}

void installLogMsgFilterSwitch(ILogMsgHandler * handler, ILogMsgFilter * switchFilter, ILogMsgFilter * newFilter)
{
    queryLogMsgManager()->changeMonitorFilterOwn(handler, getSwitchLogMsgFilterOwn(switchFilter, newFilter, queryLogMsgManager()->getMonitorFilter(handler)));
}

ILogMsgHandler * getLogMsgHandlerFromPTree(IPropertyTree * tree)
{
    StringBuffer type;
    tree->getProp("@type", type);
    unsigned fields = MSGFIELD_all;
    char const * fstr = tree->queryProp("@fields");
    if(fstr)
    {
        if(isdigit(fstr[0]))
            fields = atoi(fstr);
        else
            fields = LogMsgFieldsFromAbbrevs(fstr);
    }
    if(strcmp(type.str(), "stderr")==0)
        return getHandleLogMsgHandler(stderr, fields, tree->hasProp("@writeXML"));
    else if(strcmp(type.str(), "file")==0)
    {
        StringBuffer filename;
        tree->getProp("@filename", filename);
        if(tree->hasProp("@headertext"))
        {
            StringBuffer headertext;
            tree->getProp("@headertext", headertext);
            return getFileLogMsgHandler(filename.str(), headertext.str(), fields, !(tree->hasProp("@writeTable")), tree->hasProp("@append"), tree->hasProp("@flushes"));
        }
        else
            return getFileLogMsgHandler(filename.str(), 0, fields, !(tree->hasProp("@writeTable")), tree->hasProp("@append"), tree->hasProp("@flushes"));
    }
    else if(strcmp(type.str(), "binary")==0)
    {
        StringBuffer filename;
        tree->getProp("@filename", filename);
        return getBinLogMsgHandler(filename.str(), tree->hasProp("@append"));
    }
    else assertex(!"getLogMsgFilterFromPTree : unrecognized LogMsgHandler type");
    return LINK(theStderrHandler);
}

ILogMsgHandler * attachStandardFileLogMsgMonitor(const char * filename, const char * headertext, unsigned fields, unsigned audiences, unsigned classes, LogMsgDetail detail, bool writeXML, bool append, bool flushes, bool local)
{
#ifdef FILE_LOG_ENABLES_QUEUEUING
    queryLogMsgManager()->enterQueueingMode();
#endif
    ILogMsgFilter * filter = getCategoryLogMsgFilter(audiences, classes, detail, local);
    ILogMsgHandler * handler = getFileLogMsgHandler(filename, headertext, fields, writeXML, append, flushes);
    queryLogMsgManager()->addMonitorOwn(handler, filter);
    return handler;
}

ILogMsgHandler * attachStandardBinLogMsgMonitor(const char * filename, unsigned audiences, unsigned classes, LogMsgDetail detail, bool append, bool local)
{
#ifdef FILE_LOG_ENABLES_QUEUEUING
    queryLogMsgManager()->enterQueueingMode();
#endif
    ILogMsgFilter * filter = getCategoryLogMsgFilter(audiences, classes, detail, local);
    ILogMsgHandler * handler = getBinLogMsgHandler(filename, append);
    queryLogMsgManager()->addMonitorOwn(handler, filter);
    return handler;
}

ILogMsgHandler * attachStandardHandleLogMsgMonitor(FILE * handle, unsigned fields, unsigned audiences, unsigned classes, LogMsgDetail detail, bool writeXML, bool local)
{
    ILogMsgFilter * filter = getCategoryLogMsgFilter(audiences, classes, detail, local);
    ILogMsgHandler * handler = getHandleLogMsgHandler(handle, fields, writeXML);
    queryLogMsgManager()->addMonitorOwn(handler, filter);
    return handler;
}

ILogMsgHandler * attachLogMsgMonitorFromPTree(IPropertyTree * tree)
{
    Owned<IPropertyTree> handlertree = tree->getPropTree("handler");
    Owned<IPropertyTree> filtertree = tree->getPropTree("filter");
    ILogMsgHandler * handler = getLogMsgHandlerFromPTree(handlertree);
    ILogMsgFilter * filter = getLogMsgFilterFromPTree(filtertree);
    queryLogMsgManager()->addMonitorOwn(handler, filter);
    return handler;
}

void attachManyLogMsgMonitorsFromPTree(IPropertyTree * tree)
{
    Owned<IPropertyTreeIterator> iter = tree->getElements("monitor");
    ForEach(*iter)
        attachLogMsgMonitorFromPTree(&(iter->query()));
}

// Standard categories and unknown jobInfo

const LogMsgCategory MCdisaster(MSGAUD_all, MSGCLS_disaster);
const LogMsgCategory MCuserError(MSGAUD_user, MSGCLS_error);
const LogMsgCategory MCoperatorError(MSGAUD_operator, MSGCLS_error);
const LogMsgCategory MCinternalError(MSGAUD_programmer, MSGCLS_error, 1);
const LogMsgCategory MCuserWarning(MSGAUD_user, MSGCLS_warning);
const LogMsgCategory MCoperatorWarning(MSGAUD_operator, MSGCLS_warning);
const LogMsgCategory MCinternalWarning(MSGAUD_programmer, MSGCLS_warning, 1);
const LogMsgCategory MCuserProgress(MSGAUD_user, MSGCLS_progress);
const LogMsgCategory MCoperatorProgress(MSGAUD_operator, MSGCLS_progress);
const LogMsgCategory MCdebugProgress(MSGAUD_programmer, MSGCLS_progress);
const LogMsgCategory MCuserInfo(MSGAUD_user, MSGCLS_information);
const LogMsgCategory MCdebugInfo(MSGAUD_programmer, MSGCLS_information);
const LogMsgCategory MCstats(MSGAUD_operator, MSGCLS_progress);
const LogMsgCategory MCoperatorInfo(MSGAUD_operator, MSGCLS_information);
const LogMsgCategory MClegacy(MSGAUD_legacy, MSGCLS_legacy, DefaultDetail);

const LogMsgJobInfo unknownJob(UnknownJob, UnknownUser);

// Calls to make, remove, and return the manager, standard handler, pass all/none filters, reporter array

PassAllLogMsgFilter * thePassAllFilter;
PassLocalLogMsgFilter * thePassLocalFilter;
PassNoneLogMsgFilter * thePassNoneFilter;
HandleLogMsgHandlerTable * theStderrHandler;
CLogMsgManager * theManager;
CSysLogEventLogger * theSysLogEventLogger;
LogMsgComponentReporter * theReporters[MSGCOMP_NUMBER];

MODULE_INIT(INIT_PRIORITY_JLOG)
{
    thePassAllFilter = new PassAllLogMsgFilter();
    thePassLocalFilter = new PassLocalLogMsgFilter();
    thePassNoneFilter = new PassNoneLogMsgFilter();
    theStderrHandler = new HandleLogMsgHandlerTable(stderr, MSGFIELD_STANDARD);
    theSysLogEventLogger = new CSysLogEventLogger;
    theManager = new CLogMsgManager();
    theManager->resetMonitors();
    for(unsigned compo = 0; compo<MSGCOMP_NUMBER; compo++)
        theReporters[compo] = new LogMsgComponentReporter(compo);
    return true;
}
MODULE_EXIT()
{
    for(unsigned compo = 0; compo<MSGCOMP_NUMBER; compo++)
    {
        delete theReporters[compo];
        theReporters[compo] = NULL;
    }
    delete theManager;
    delete theSysLogEventLogger;
    delete theStderrHandler;
    delete thePassNoneFilter;
    delete thePassLocalFilter;
    delete thePassAllFilter;
    theManager = NULL;
    theSysLogEventLogger = NULL;
    theStderrHandler = NULL;
    thePassNoneFilter = NULL;
    thePassLocalFilter = NULL;
    thePassAllFilter = NULL;
}

ILogMsgManager * queryLogMsgManager()
{
    return theManager;
}

ILogMsgHandler * queryStderrLogMsgHandler()
{
    return theStderrHandler;
}

LogMsgComponentReporter * queryLogMsgComponentReporter(unsigned compo)
{
    return theReporters[compo];
}

ILogMsgManager * createLogMsgManager() // use with care! (needed by mplog listener facility)
{
    return new CLogMsgManager();
}

// Event Logging

ISysLogEventLogger * querySysLogEventLogger()
{
    return theSysLogEventLogger;
}

ILogMsgHandler * getSysLogMsgHandler(unsigned fields)
{
    return new SysLogMsgHandler(theSysLogEventLogger, fields); 
}

#ifdef _WIN32

#include <WINNT.H>
#include "jelog.h"

struct AuditTypeWin32Data
{
public:
    unsigned eventtype;
    unsigned categoryid;
    unsigned eventid;
};

#define CATEGORY_AUDIT_FUNCTION_REQUIRED
#define AUDIT_TYPES_BEGIN AuditTypeWin32Data auditTypeDataMap[NUM_AUDIT_TYPES+1] = {
#define MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level) {type, categoryid, eventid},
#define AUDIT_TYPES_END {0, 0, 0} };
#include "jelogtype.hpp"
#undef CATEGORY_AUDIT_FUNCTION_REQUIRED
#undef AUDIT_TYPES_BEGIN
#undef MAKE_AUDIT_TYPE
#undef AUDIT_TYPES_END

CSysLogEventLogger::CSysLogEventLogger() : hEventLog(0)
{
}

bool CSysLogEventLogger::log(AuditType auditType, char const * msg, size32_t datasize, void const * data)
{
    assertex(auditType < NUM_AUDIT_TYPES);
    AuditTypeWin32Data const & typeData = auditTypeDataMap[auditType];
    return win32Report(typeData.eventtype, typeData.categoryid, typeData.eventid, msg, datasize, data);
}

bool CSysLogEventLogger::win32Report(unsigned eventtype, unsigned category, unsigned eventid, const char * msg, size32_t datasize, const void * data)
{
    if (hEventLog==0) {
        // MORE - this doesn't work on Vista/Win7 as can't copy to system32...
        // Perhaps we should just kill this code
        char path[_MAX_PATH+1];
        GetEnvironmentVariable("SystemRoot",path,sizeof(path));
        strcat(path,"\\System32\\JELOG.dll");
        Owned<IFile> file = createIFile(path);
        try {
            if (!file->exists()) {
                char src[_MAX_PATH+1];
                LPTSTR tail;
                DWORD res = SearchPath(NULL,"JELOG.DLL",NULL,sizeof(src),src,&tail);
                if (res>0) 
                    copyFile(path,src);
                else
                    throw makeOsException(GetLastError());
            }
        }
        catch (IException *e)
        {
            EXCLOG(e, "reportEventLog: Could not install JELOG.DLL");
            hEventLog=(HANDLE)-1;
            e->Release();
            return false;

        }
        HKEY hk;
        if (RegCreateKeyEx(HKEY_LOCAL_MACHINE,"SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\Seisint", 
                           NULL, NULL, 0, KEY_ALL_ACCESS, NULL, &hk, NULL)==0) {
            DWORD sizedata = 0;
            DWORD type = REG_EXPAND_SZ;
            if ((RegQueryValueEx(hk,"EventMessageFile",NULL, &type, NULL, &sizedata)!=0)||!sizedata) {
                StringAttr str("%SystemRoot%\\System32\\JELOG.dll"); 
                RegSetValueEx(hk,"EventMessageFile", 0, REG_EXPAND_SZ, (LPBYTE) str.get(), (DWORD)str.length() + 1);
                RegSetValueEx(hk,"CategoryMessageFile", 0, REG_EXPAND_SZ, (LPBYTE) str.get(), (DWORD)str.length() + 1);
                DWORD dwData = EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE | EVENTLOG_INFORMATION_TYPE | EVENTLOG_AUDIT_SUCCESS | EVENTLOG_AUDIT_FAILURE; 
                RegSetValueEx(hk, "TypesSupported", 0, REG_DWORD,  (LPBYTE) &dwData,  sizeof(DWORD));
                dwData = 16;
                RegSetValueEx(hk, "CategoryCount", 0, REG_DWORD,  (LPBYTE) &dwData,  sizeof(DWORD));
            }
            RegCloseKey(hk); 
        }
        hEventLog = RegisterEventSource(NULL,"Seisint");
        if (!hEventLog) {
            OERRLOG("reportEventLog: Could not register Seisint event source");
            hEventLog=(HANDLE)-1;
            return false;
        }
    }
    if (hEventLog==(HANDLE)-1)
        return false;
    assertex((unsigned)eventtype<=16);
    if (!data)
        datasize = 0;
    else if (!datasize)
        data = NULL;
#if 1 //useful for debugging...
    ReportEvent(hEventLog, eventtype, category, eventid, NULL, 1, datasize, &msg, (LPVOID)data);
#else
    if(datasize)
    {
        char * buff = (char *)malloc(datasize*3+1);
        unsigned char const * cdata = (unsigned char *)data;
        unsigned i;
        for(i=0; i<datasize; i++)
            sprintf(buff+i*3, "%02X ", cdata[i]);
        buff[datasize*3-1] = 0;
        DBGLOG("ReportEvent: type=%X categoryid=%X eventid=%X msg='%s' data=[%s]", eventtype, category, eventid, msg, buff);
        free(buff);
    }
    else
        DBGLOG("ReportEvent: type=%X categoryid=%X eventid=%X msg='%s'", eventtype, category, eventid, msg);
#endif
    return true;
}

CSysLogEventLogger::~CSysLogEventLogger()
{
    if (hEventLog!=0) 
        DeregisterEventSource(hEventLog); 
}

#else

#include <syslog.h>

#define CATEGORY_AUDIT_FUNCTION_REQUIRED
#define AUDIT_TYPES_BEGIN int auditTypeDataMap[NUM_AUDIT_TYPES+1] = {
#define MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level) level, 
#define AUDIT_TYPES_END 0 };
#include "jelogtype.hpp"
#undef CATEGORY_AUDIT_FUNCTION_REQUIRED
#undef AUDIT_TYPES_BEGIN
#undef MAKE_AUDIT_TYPE
#undef AUDIT_TYPES_END

CSysLogEventLogger::CSysLogEventLogger() : dataLogUsed(false), dataLogName(0), dataLogFile(-1)
{
    StringBuffer folder;
    const char * processName = splitDirTail(queryCurrentProcessPath(), folder);
    if (!processName||!*processName)
        processName = "hpcc";
    openlog(processName, LOG_PID, LOG_USER);
}

CSysLogEventLogger::~CSysLogEventLogger()
{
    if(dataLogFile != -1)
        close(dataLogFile);
    if(dataLogName)
        delete [] dataLogName;
    closelog();
}

bool CSysLogEventLogger::log(AuditType auditType, const char *msg, size32_t datasize, const void * data)
{
    assertex(auditType < NUM_AUDIT_TYPES);
    int level = auditTypeDataMap[auditType];
    return linuxReport(level, msg, datasize, data);
}

bool CSysLogEventLogger::linuxReport(int level, const char * msg, size32_t datasize, const void * data)
{
    if (!data)
        datasize = 0;
    else if (!datasize)
        data = NULL;
    bool ret = true;
#if 1 //useful for debugging...
    if(data)
    {
        if(!dataLogUsed)
            openDataLog();
        if(dataLogFile != -1)
        {
            int fpos = writeDataLog(datasize, (byte const *)data);
            if(fpos != -1)
                syslog(level, "%s [0x%X bytes of data at %s byte 0x%X]", msg, datasize, dataLogName, fpos);
            else
                syslog(level, "%s [could not write 0x%X bytes of data to %s]", msg, datasize, dataLogName);
        }
        else
        {
            ret = false;
            syslog(level, "%s [could not open file of form %s to write data]", msg, AUDIT_DATA_LOG_TEMPLATE);
        }
    }
    else
    {
        syslog(level, "%s", msg);
    }
#else
    if(datasize)
    {
        char * buff = (char *)malloc(datasize*3+1);
        unsigned char const * cdata = (unsigned char *)data;
        unsigned i;
        for(i=0; i<datasize; i++)
            sprintf(buff+i*3, "%02X ", cdata[i]);
        buff[datasize*3-1] = 0;
        DBGLOG("syslog: priority=%X msg='%s' data=[%s]", level, msg, buff);
        free(buff);
    }
    else
        DBGLOG("syslog: priority=%X msg='%s'", level, msg);
#endif
    return ret;
}

void CSysLogEventLogger::openDataLog()
{
    CriticalBlock block(dataLogLock);
    dataLogUsed = true;
    unsigned len = strlen(AUDIT_DATA_LOG_TEMPLATE);
    dataLogName = new char[len+1];
    strcpy(dataLogName, AUDIT_DATA_LOG_TEMPLATE);
    dataLogFile = mkstemp(dataLogName);
}

int CSysLogEventLogger::writeDataLog(size32_t datasize, byte const * data)
{
    CriticalBlock block(dataLogLock);
    off_t fpos = lseek(dataLogFile, 0, SEEK_CUR);
    while(datasize > 0)
    {
        ssize_t written = write(dataLogFile, data, datasize);
        if (written == -1)
            return -1;
        data += written;
        datasize -= written;
    }
#ifndef _WIN32
#ifdef F_FULLFSYNC
    fcntl(dataLogFile, F_FULLFSYNC);
#else
    fdatasync(dataLogFile);
#endif
#ifdef POSIX_FADV_DONTNEED
    posix_fadvise(dataLogFile, 0, 0, POSIX_FADV_DONTNEED);
#endif
#endif
    return fpos;
}

#endif

void SysLogMsgHandler::handleMessage(const LogMsg & msg) const
{
    AuditType type = categoryToAuditType(msg.queryCategory());
    StringBuffer text;
    msg.toStringPlain(text, fields);
    logger->log(type, text.str());
}

void SysLogMsgHandler::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(ipt_caseInsensitive);
    handlerTree->setProp("@type", "audit");
    tree->addPropTree("handler", handlerTree);
}

// Default implementations of the functions in IContextLogger interface

void IContextLogger::CTXLOG(const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    CTXLOGva(format, args);
    va_end(args);
}

void IContextLogger::logOperatorException(IException *E, const char *file, unsigned line, const char *format, ...) const
{
    va_list args;
    va_start(args, format);
    logOperatorExceptionVA(E, file, line, format, args);
    va_end(args);
}

class DummyLogCtx : implements IContextLogger
{
private:
    StringAttr globalId;
    StringBuffer localId;
    StringAttr globalIdHeader;
    StringAttr callerIdHeader;

public:
    // It's a static object - we don't want to actually link-count it...
    virtual void Link() const {}
    virtual bool Release() const { return false; }

    virtual void CTXLOGva(const char *format, va_list args) const __attribute__((format(printf,2,0)))
    {
        StringBuffer ss;
        ss.valist_appendf(format, args);
        DBGLOG("%s", ss.str());
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const __attribute__((format(printf,5,0)))
    {
        StringBuffer ss;
        ss.append("ERROR");
        if (E)
            ss.append(": ").append(E->errorCode());
        if (file)
            ss.appendf(": %s(%d) ", sanitizeSourceFile(file), line);
        if (E)
            E->errorMessage(ss.append(": "));
        if (format)
            ss.append(": ").valist_appendf(format, args);
        LOG(MCoperatorProgress, unknownJob, "%s", ss.str());
    }
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value) const
    {
    }
    virtual void mergeStats(const CRuntimeStatisticCollection &from) const
    {
    }
    virtual unsigned queryTraceLevel() const
    {
        return 0;
    }
    virtual void setGlobalId(const char *id, SocketEndpoint &ep, unsigned pid)
    {
        globalId.set(id);
        appendLocalId(localId.clear(), ep, pid);
    }
    virtual const char *queryGlobalId() const
    {
        return globalId.get();
    }
    virtual const char *queryLocalId() const
    {
        return localId.str();
    }
    virtual void setHttpIdHeaders(const char *global, const char *caller)
    {
        if (global && *global)
            globalIdHeader.set(global);
        if (caller && *caller)
            callerIdHeader.set(caller);
    }
    virtual const char *queryGlobalIdHttpHeader() const
    {
        return globalIdHeader.str();
    }
    virtual const char *queryCallerIdHttpHeader() const
    {
        return callerIdHeader.str();
    }
} dummyContextLogger;

extern jlib_decl const IContextLogger &queryDummyContextLogger()
{
    return dummyContextLogger;
}

extern jlib_decl IContextLogger &updateDummyContextLogger()
{
    return dummyContextLogger;
}

extern jlib_decl StringBuffer &appendLocalId(StringBuffer &s, const SocketEndpoint &ep, unsigned pid)
{
    static unsigned short cnt = msTick();

    MemoryBuffer data;
    data.append(ep.iphash());
    if (pid>0)
        data.append(pid);
    else
        data.append(ep.port);
    data.append(++cnt);

    size_t b58Length = data.length() * 2;
    StringBuffer id;

    //base58 works well as a human readable format, i.e. so customers can easily report the id that was recieved
    if (b58enc(id.reserve(b58Length), &b58Length, data.toByteArray(), data.length()) && b58Length > 1)
    {
        id.setLength(b58Length);
        s.append(id);
    }
    return s;
}

extern jlib_decl void UseSysLogForOperatorMessages(bool use)
{
    static ILogMsgHandler *msgHandler=NULL;
    if (use==(msgHandler!=NULL))
        return;
    if (use) {
        msgHandler = getSysLogMsgHandler();
        ILogMsgFilter * operatorFilter = getCategoryLogMsgFilter(MSGAUD_operator, MSGCLS_all, DefaultDetail, true);  
        queryLogMsgManager()->addMonitorOwn(msgHandler, operatorFilter);
    }
    else {
        queryLogMsgManager()->removeMonitor(msgHandler);
        msgHandler = NULL;
    }
}

extern jlib_decl void AuditSystemAccess(const char *userid, bool success, char const * msg,...)
{
    va_list args;
    va_start(args, msg);
    VStringBuffer s("User %s: ", userid);
    SYSLOG((success) ? AUDIT_TYPE_ACCESS_SUCCESS : AUDIT_TYPE_ACCESS_FAILURE, s.valist_appendf(msg, args).str());
    va_end(args);
}

//--------------------------------------------------------------

class jlib_decl CComponentLogFileCreator : implements IComponentLogFileCreator, public CInterface
{
private:
    StringBuffer component;

    //filename parts
    StringBuffer prefix;
    StringBuffer name;
    StringBuffer postfix;
    StringBuffer extension;
    StringBuffer fullFileSpec;

    bool         createAlias;
    StringBuffer aliasName;

    StringBuffer logDirSubdir;

    bool         rolling;

    //ILogMsgHandler fields
    bool         append;
    bool         flushes;
    unsigned     msgFields;

    //ILogMsgFilter fields
    unsigned     msgAudiences;
    unsigned     msgClasses;
    LogMsgDetail maxDetail;
    bool         local;

    //available after logging started
    StringBuffer logDir;        //access via queryLogDir()
    StringBuffer aliasFileSpec; //access via queryAliasFileSpec()
    StringBuffer expandedLogSpec;//access via queryLogFileSpec()

private:
    void setDefaults()
    {
        rolling = true;
        append = true;
        flushes = true;
        const char *logFields = queryEnvironmentConf().queryProp("logfields");
        if (!isEmptyString(logFields))
            msgFields = LogMsgFieldsFromAbbrevs(logFields);
        else
            msgFields = MSGFIELD_STANDARD;
        msgAudiences = MSGAUD_all;
        msgClasses = MSGCLS_all;
        maxDetail = DefaultDetail;
        name.set(component); //logfile defaults to component name. Change via setName(), setPrefix() and setPostfix()
        extension.set(".log");
        local = false;
        createAlias = true;
    }

public:
    IMPLEMENT_IINTERFACE;
    CComponentLogFileCreator(IPropertyTree * _properties, const char *_component) : component(_component)
    {
        setDefaults();
        if (_properties && !getConfigurationDirectory(_properties->queryPropTree("Directories"), "log", _component, _properties->queryProp("@name"), logDir))
            _properties->getProp("@logDir", logDir);
    }

    CComponentLogFileCreator(const char *_logDir, const char *_component) : component(_component), logDir(_logDir)
    {
        setDefaults();
    }

    CComponentLogFileCreator(const char *_component) : component(_component)
    {
        setDefaults();
        if (!getConfigurationDirectory(NULL, "log", _component, _component, logDir))
        {
            appendCurrentDirectory(logDir,false);
        }
    }

    //set methods
    void setExtension(const char * _ext)     { extension.set(_ext); }
    void setPrefix(const char * _prefix)     { prefix.set(_prefix); }
    void setName(const char * _name)         { name.set(_name); }
    void setCompleteFilespec(const char * _fs){fullFileSpec.set(_fs); setExtension(NULL); setRolling(false);}
    void setPostfix(const char * _postfix)   { postfix.set(_postfix); }
    void setCreateAliasFile(bool _create)    { createAlias = _create; }
    void setAliasName(const char * _aliasName)   { aliasName.set(_aliasName); }
    void setLogDirSubdir(const char * _subdir)   { logDirSubdir.set(_subdir); }
    void setRolling(const bool _rolls)       { rolling = _rolls; }

    //ILogMsgHandler fields
    void setAppend(const bool _append)       { append = _append; }
    void setFlushes(const bool _flushes)     { flushes = _flushes; }
    void setMsgFields(const unsigned _fields){ msgFields = _fields; }

    //ILogMsgFilter fields
    void setMsgAudiences(const unsigned _audiences){ msgAudiences = _audiences; }
    void setMsgClasses(const unsigned _classes)    { msgClasses = _classes; }
    void setMaxDetail(const LogMsgDetail _maxDetail)  { maxDetail = _maxDetail; }
    void setLocal(const bool _local)               { local = _local; }

    //query methods (not valid until logging started)
    const char * queryLogDir() const        { return logDir.str(); }
    const char * queryLogFileSpec() const   { return expandedLogSpec.str(); }
    const char * queryAliasFileSpec() const { return aliasFileSpec.str(); }

    ILogMsgHandler * beginLogging()
    {
        //build directory path
        StringBuffer logFileSpec;
        if (!fullFileSpec.length())//user specify complete logfile specification?
        {
            if (!logDir.length())
            {
                appendCurrentDirectory(logDir,false).append(PATHSEPSTR).append("logs");
                OWARNLOG("No logfile directory specified - logs will be written locally to %s", logDir.str());
            }

            makeAbsolutePath(logDir);

            //build log file name (without date string or extension)
            StringBuffer logFileName;
            if (prefix.length())
                logFileName.append(prefix).append(".");
            logFileName.append(name);
            if (postfix.length())
                logFileName.append(".").append(postfix);

            //build log file spec
            if (logDirSubdir.length())
                logDir.append(PATHSEPCHAR).append(logDirSubdir);//user specified subfolder
            logFileSpec.append(logDir).append(PATHSEPCHAR).append(logFileName);

            //build alias file spec
            if (createAlias)
            {
                if (aliasName.length()==0)
                    aliasName.set(logFileName);
                aliasFileSpec.append(logDir).append(PATHSEPCHAR).append(aliasName).append(extension);
            }
        }
        else
            makeAbsolutePath(fullFileSpec);

        ILogMsgHandler * lmh;
        if (rolling)
        {
            lmh = getRollingFileLogMsgHandler(logFileSpec.str(), extension, msgFields, append, flushes, NULL, aliasFileSpec.str(), true);
        }
        else
        {
            StringBuffer lfs;
            if (fullFileSpec.length())
                lfs.set(fullFileSpec);
            else
                lfs.set(logFileSpec.append(extension).str());
            lmh = getFileLogMsgHandler(lfs.str(), NULL, msgFields, false);
        }
        lmh->getLogName(expandedLogSpec);
        queryLogMsgManager()->addMonitorOwn( lmh, getCategoryLogMsgFilter(msgAudiences, msgClasses, maxDetail, local));
        return lmh;
    }
};

IComponentLogFileCreator * createComponentLogFileCreator(IPropertyTree * _properties, const char *_component)
{
    return new CComponentLogFileCreator(_properties, _component);
}

IComponentLogFileCreator * createComponentLogFileCreator(const char *_logDir, const char *_component)
{
    return new CComponentLogFileCreator(_logDir, _component);
}

IComponentLogFileCreator * createComponentLogFileCreator(const char *_component)
{
    return new CComponentLogFileCreator(_component);
}
