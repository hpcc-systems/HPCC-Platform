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
#ifndef HQLREMOTE_INCL
#define HQLREMOTE_INCL
#include "workunit.hpp"

typedef unsigned __int64 timestamp_t;

//An opaque type which can be linked.
interface IEclUser : public IInterface
{
};

interface IXmlEclRepository : public IInterface
{
public:
    virtual int getModules(StringBuffer & xml, IEclUser * user, timestamp_t timestamp) = 0;
    virtual int getAttributes(StringBuffer & xml, IEclUser * user, const char * modname, const char * attribute, int version, unsigned char infoLevel, const char * snapshot, bool sandbox4snapshot) = 0;
    virtual void noteQuery(const char * query) = 0;
};

interface IXmlEclRepositoryEx : extends IXmlEclRepository
{
public:
    virtual int updateRepository(const char * user, const char * text) = 0;
    virtual int renameAttribute(const char * user, const char * modname, const char * attribute, const char * newModule, const char * newAttribute) = 0;
    virtual int wipeSandbox(const char * user, const char * modname) = 0;
    virtual int recreateHistory(const char * user, const char * modname, const char * asoftime, bool overwrite) = 0;
    virtual int getAttributeHistory(const char * user, const char * modname, const char * attribute, char * & text, const char * usercreated, const char * userchanged, const char * fromtime, const char * totime, int fromversion, int toversion) = 0;
    virtual int findAttributeText(const char * user, const char * modname, const char * attribute, const char * pattern, char * & text) = 0;
    virtual void createSnapshot(const char * user, const char * name, const char * timestamp) = 0;
    virtual void getSnapshots(const char * user, char * & text) = 0;
};


extern HQL_API IXmlEclRepository * createReplayRepository(IPropertyTree * xml);
extern "C" HQL_API IEclRepository * attachLocalServer(IEclUser * user, IXmlEclRepository & repository, const char * snapshot, bool sandbox4snapshot);
extern "C" HQL_API IEclRepository * attachLoggingServer(IEclUser * user, IXmlEclRepository & repository, IWorkUnit* workunit, const char * snapshot, bool sandbox4snapshot);
extern "C" HQL_API IEclRepository * createXmlDataServer(IPropertyTree * _xml, IEclRepository * defaultDataServer);

#endif

