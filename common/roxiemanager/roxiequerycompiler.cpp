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

#pragma warning (disable : 4786)

#include "jlib.hpp"
#include "roxiequerycompiler.hpp"

class CRoxieQueryCompileInfo : public CInterface, implements IRoxieQueryCompileInfo
{
private:
    unsigned memoryLimit;
    unsigned wuTimeOut;
    unsigned timeLimit;
    unsigned warnTimeLimit;

    bool poolGraphs;
    bool highPriority;

    StringBuffer repositoryLabel;
    StringBuffer moduleName;
    StringBuffer defaultStyleName;
    StringBuffer wuDebugOptions;

    StringBuffer userName;
    StringBuffer password;
    StringBuffer jobName;
    StringBuffer ecl;
    StringBuffer appName;
    StringBuffer clusterName;
    int queryPriority;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryCompileInfo(const char *_ecl, const char *_jobName, const char *_clusterName, const char *_appName)
        :ecl(_ecl), jobName(_jobName), clusterName(_clusterName)
    {
        memoryLimit = 0;
        wuTimeOut = 0;
        timeLimit = 0;
        warnTimeLimit = 0;
        poolGraphs = false;
        highPriority = false;
        queryPriority = UNKNOWN_PRIORITY;
        
        if (_appName && *_appName)
            appName.append(_appName);
        else
            appName.append("RoxieManager");
    }

    virtual void setMemoryLimit(unsigned val) { memoryLimit = val; }
    virtual unsigned getMemoryLimit() { return memoryLimit; }
    
    virtual void setWuTimeOut(unsigned val) { wuTimeOut = val; }
    virtual unsigned getWuTimeOut() { return wuTimeOut; }

    virtual void setTimeLimit(unsigned val) { timeLimit = val; }
    virtual unsigned getTimeLimit() { return timeLimit; }

    virtual void setWarnTimeLimit(unsigned val) { warnTimeLimit = val; }
    virtual unsigned getWarnTimeLimit() { return warnTimeLimit; }

    virtual void setPoolGraphs(bool val) { poolGraphs = val; }
    virtual bool getPoolGraphs() { return poolGraphs; }

    virtual void setHighPriority(bool val) { highPriority = val; }
    virtual bool getHighPriority() { return highPriority; }

    virtual void setRepositoryLabel(const char *val) { repositoryLabel.append(val); }
    virtual const char * queryRepositoryLabel() { return repositoryLabel.str(); }

    virtual void enableWebServiceInfoRetrieval(const char *_moduleName, const char *_defaultStyleName) 
        { moduleName.append(_moduleName); defaultStyleName.append(_defaultStyleName);}

    virtual void setQueryPriority(int val) { queryPriority = val; }
    virtual int getQueryPriority() { return queryPriority; }

    virtual void setWuDebugOptions(const char *val) { wuDebugOptions.append(val); }
    virtual const char * queryWuDebugOptions() { return wuDebugOptions.str(); }

    virtual const char * queryUserId() { return userName.str(); }
    virtual const char * queryPassword() { return password.str(); }
    virtual const char * queryJobName() { return jobName.str(); }
    virtual const char * queryEcl() { return ecl.str(); }
    virtual const char * queryAppName() { return appName.str(); }
    virtual const char * queryClusterName() { return clusterName.str(); }
    virtual const char * queryDefaultStyleName() { return defaultStyleName.str(); }
    virtual const char * queryModuleName() { return moduleName.str(); }
};




class CRoxieQueryCompiler : public CInterface, implements IRoxieQueryCompiler
{
private:
    SocketEndpoint ep;

    void processCompilationErrors(IConstWorkUnit *workunit, SCMStringBuffer &errors)
    {
        if (workunit->getExceptionCount())
        {
            Owned<IConstWUExceptionIterator> exceptions = &workunit->getExceptions();
            StringArray errorStr;
            StringArray warnStr;
            ForEach (*exceptions)
            {
                IConstWUException &exception = exceptions->query();

                unsigned exceptCode = exception.getExceptionCode();
                SCMStringBuffer name;
                exception.getExceptionFileName(name);

                unsigned lineNo = exception.getExceptionLineNo();
                unsigned column = exception.getExceptionColumn();

                SCMStringBuffer source, message;
                exception.getExceptionSource(source);
                exception.getExceptionMessage(message);

                if (exception.getSeverity() == ExceptionSeverityWarning)
                {
                    StringBuffer err;
                    if ( (lineNo != 0) || (column != 0))
                        err.appendf("WARNING: %s (%d, %d): %d %s", name.str(), lineNo, column, exceptCode, message.str());
                    else
                        err.appendf("WARNING: %s: %d %s", name.str(), exceptCode, message.str());
                    warnStr.append(err.str());
                }
                else if (exception.getSeverity() == ExceptionSeverityError)
                {
                    StringBuffer err;
                    if ( (lineNo != 0) || (column != 0))
                        err.appendf("ERROR: %s (%d, %d): %d %s", name.str(), lineNo, column, exceptCode, message.str());
                    else
                        err.appendf("ERROR: %s: %d %s", name.str(), exceptCode, message.str());
                    errorStr.append(err.str());
                }
            }

            ForEachItemIn(err_idx, errorStr)
            {
                const char *err = errorStr.item(err_idx);
                errors.s.appendf("%s\n", err);
            }

            ForEachItemIn(warn_idx, warnStr)
            {
                const char *err = warnStr.item(warn_idx);
                errors.s.appendf("%s\n", err);
            }

            errors.s.appendf("%d error(s),  %d warning(s)\n", errorStr.ordinality(), warnStr.ordinality());
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryCompiler()
    {
    }

    IConstWorkUnit *createWorkunit(SCMStringBuffer &wuid, const char *userName, const char *queryAppName)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> workunit = factory->createWorkUnit(NULL, queryAppName, userName);
        workunit->getWuid(wuid);
        workunit->setState(WUStateUnknown);
        workunit->setUser(userName);
        return workunit.getClear();
    }

    IConstWorkUnit *compileEcl(SCMStringBuffer &wuid, const char *userName, const char *password, IRoxieQueryCompileInfo &compileInfo, IRoxieQueryProcessingInfo &processingInfo, SCMStringBuffer &status)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        {
            Owned<IWorkUnit> workunit;
            
            if (wuid.length())
                workunit.setown(factory->updateWorkUnit(wuid.str()));
            else
                workunit.setown(factory->createWorkUnit(NULL, compileInfo.queryAppName(), userName));

            workunit->setDebugValue("RoxieConfigStatus", "Compiling...", true);

            Owned<IWUQuery> query = workunit->updateQuery();
            query->setQueryText(compileInfo.queryEcl());
            query->setQueryType(QueryTypeEcl);

            const char *queryJobName = compileInfo.queryJobName();
            if (queryJobName && *queryJobName)
                workunit->setJobName(queryJobName);

            workunit->setAction(WUActionCompile);
            workunit->setUser(userName);
            workunit->getWuid(wuid);

            // set generic debug options
            StringBuffer options(compileInfo.queryWuDebugOptions());
            if (options.length())
            {
                Owned<IPropertyTree> dbgOptions = createPTreeFromXMLString(options, ipt_caseInsensitive);
                Owned<IPropertyTreeIterator> iter = dbgOptions->getElements("Option");
                ForEach(*iter)
                {
                    IPropertyTree &item = iter->query();
                    const char *name = item.queryProp("@name");
                    const char *value = item.queryProp("@value");
                    workunit->setDebugValue(name, value, true);
                }
            }

            // set specific debug options
            workunit->setDebugValueInt("forceRoxie", 1, true);
            workunit->setDebugValueInt("traceRowXml", 1, true);
            workunit->setDebugValue("targetClusterType", "roxie", true);

            workunit->setClusterName(compileInfo.queryClusterName()); // MORE - eclserver should not require cluster set for roxy compiles (probably)

            if (compileInfo.getHighPriority())
                workunit->setPriority(PriorityClassHigh);

            int queryPriority = compileInfo.getQueryPriority();
            if (queryPriority != UNKNOWN_PRIORITY)  // MORE - if set use queryPriority to set wu priority
            {
                workunit->setDebugValueInt("queryPriority", queryPriority, true);
                if (queryPriority == LOW_PRIORITY)
                    workunit->setPriority(PriorityClassLow);
                else
                    workunit->setPriority(PriorityClassHigh);
            }
            else
            {
                if (!compileInfo.getHighPriority())
                    workunit->setDebugValue("queryPriority", "High", true);
            }

            workunit->setDebugValueInt("memoryLimit", compileInfo.getMemoryLimit(), true);
            workunit->setDebugValueInt("timeLimit", compileInfo.getTimeLimit(), true);
            workunit->setDebugValueInt("warnTimeLimit", compileInfo.getWarnTimeLimit(), true);
            workunit->setDebugValueInt("poolGraphs", compileInfo.getPoolGraphs(), true);

            workunit->setDebugValue("comment", processingInfo.queryComment(), true);
            workunit->setDebugValue("package", processingInfo.queryPackageName(), true);

            const char *label = compileInfo.queryRepositoryLabel();
            if (label && *label)
                workunit->setSnapshot(label);
        }
        submitWorkUnit(wuid.str(), userName, password);
        if (waitForWorkUnitToCompile(wuid.str(), compileInfo.getWuTimeOut()))
        {
            IConstWorkUnit *workunit = factory->openWorkUnit(wuid.str(), false);
            Owned<IWorkUnit> wu = &workunit->lock();
            SCMStringBuffer jn;
            wu->getJobName(jn);
            if (jn.length() == 0)
            {
                SCMStringBuffer name;
                wu->getDebugValue("name", name);
                if (name.length())
                    wu->setJobName(name.str());
                else
                    wu->setJobName(wuid.str());
            }
            processCompilationErrors(workunit, status);
            return workunit;
        }
        else
        {
            Owned<IConstWorkUnit> workunit = factory->openWorkUnit(wuid.str(), false);
            WUState wu_state = workunit->getState();

            // there was a problem, just report it - either compilation problems, eclserver timeout, etc
            processCompilationErrors(workunit, status);
            if (!status.length())  // no compilations errors so it must be something else
            {
                //StringBuffer errStr;
                if (wu_state == WUStateCompiling)
                    status.s.append("Time out trying to compile query - need to increase the wuTimeOut");
                else
                {
                    SCMStringBuffer state;
                    workunit->getStateDesc(state);
                    status.s.appendf("Unknown error: Could not compile query - make sure eclserver is running - workunit status = %s", state.str());
                }
            }
            return NULL;
        }
    }

};



IRoxieQueryCompileInfo *createRoxieQueryCompileInfo(const char *_ecl, const char *_jobName, const char *_clusterName, const char *_appName)
{
    return new CRoxieQueryCompileInfo(_ecl, _jobName, _clusterName, _appName);
}


IRoxieQueryCompiler *createRoxieQueryCompiler()
{
    return new CRoxieQueryCompiler();
}

