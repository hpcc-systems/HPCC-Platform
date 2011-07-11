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

#ifndef _ROXIEWUPROCESSOR_HPP__
#define _ROXIEWUPROCESSOR_HPP__


interface IConstWorkUnit;
interface IWorkUnit;
interface IEmbeddedXslTransformer;
interface IAttributeMetaDataResolver;

#include "jptree.hpp"
#include "esp.hpp"
#include "roxie.hpp"

#include "roxiemanager.hpp"

interface IRoxieCommunicationClient;

#define GET_LOCK_FAILURE            1100
//#define DALI_FILE_LOOKUP_TIMEOUT (1000*60*1)  // 1 minute
#define DALI_FILE_LOOKUP_TIMEOUT (1000*15*1)  // 15 seconds

enum QueryAction { DEPLOY_ATTR, DEPLOY_WU, COMPILE, ROXIE_ON_DEMAND };

interface IRoxieWuProcessor : extends IInterface
{
    virtual void retrieveFileNames(IPropertyTree *remoteQuery, IPropertyTree *remoteTopology, bool copyFileLocationInfo, const char *lookupDaliIp, const char *user, const char *password) = 0;
    virtual void retrieveFileNames(IPropertyTree *xml, IPropertyTree *remoteQuery, IPropertyTree *remoteState, IPropertyTree *remoteTopology, bool copyFileLocationInfo, const char *lookupDaliIp, const char *user, const char *password) = 0;
    virtual bool lookupFileNames(IWorkUnit *wu, IRoxieQueryProcessingInfo &processingInfo, SCMStringBuffer &status) = 0;

    virtual bool processAddRoxieFileInfoToDali(const char *src_filename, const char *dest_filename, const char *lookupDaliIp, IUserDescriptor *userdesc, StringBuffer &status) = 0;
    virtual bool processAddRoxieFileRelationshipsToDali(const char *src_filename, const char *remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc, StringBuffer &msg) = 0;
    virtual IPropertyTree *queryPackageInfo() = 0;

};

extern IRoxieWuProcessor *createRoxieWuProcessor(const char *roxieClusterName, IRoxieCommunicationClient *_roxieCommClient, int logLevel);

#endif

