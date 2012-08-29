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

