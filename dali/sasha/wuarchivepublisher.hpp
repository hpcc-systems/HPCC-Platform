/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#pragma once

#include "jlib.hpp"
#include "jstring.hpp"
#include "jdebug.hpp"
#include "jtime.hpp"
#include "jstats.h"
#include <string>
#include <vector>

// Record types passed to archive publishers.
struct ArchivedWuFileRecord
{
    StringAttr fileNameLc; // normalized to lowercase
    unsigned useCount = 0;
    bool isSuper = false;
};

struct ArchivedWuErrorRecord
{
    unsigned sequenceNo = 0;
    StringAttr errorMessage;
};

struct ArchivedWuApplicationValueRecord
{
    StringAttr application;
    StringAttr name;
    StringAttr value;
};

// Holds the searchable attributes and summary data needed to serve archived
// workunits through WsWorkunits WUQuery after the live workunit is archived.
// ECL text is excluded because it can be very large.
struct ArchivedWuRecord
{
    StringAttr wuid;
    StringAttr clusterLc;       // cluster name, lowercase
    StringAttr ownerLc;         // submitting user, lowercase
    StringAttr jobNameLc;       // job name, lowercase
    StringAttr state;           // WU state name text (for example: completed)
    bool       isProtected = false;
    bool       isPausing = false;
    int        WUAction = 0;
    cost_type  executeCost = 0;
    cost_type  fileAccessCost = 0;
    cost_type  compileCost = 0;
    cost_type  costSavingPotential = 0;
    unsigned   totalClusterTime = 0; // milliseconds (from getTotalThorTime)
    CDateTime  modifiedAt;     // workunit last-modified timestamp
    StringAttr failureDesc;

    std::vector<ArchivedWuFileRecord>     files;
    std::vector<ArchivedWuErrorRecord>    errors;
    std::vector<ArchivedWuApplicationValueRecord> applicationValues;
};

// Abstract publisher interface used by Sasha archiver
interface IArchivedWuPublisher : extends IInterface
{
    // Publish (upsert) minimal indexed metadata for an archived workunit.
    // Publisher implementations must handle publication retries independently
    // of workunit archival.
    virtual void publish(const ArchivedWuRecord & record) = 0;
};

// Factory for an archive publisher. The initial backend-neutral implementation
// returns nullptr until a concrete backend is installed.
interface IPropertyTree;

extern IArchivedWuPublisher * createArchivedWuPublisher(const IPropertyTree * config);
