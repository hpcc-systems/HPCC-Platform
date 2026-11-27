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

struct SqlConnectionInfo
{
    const char * serverName = nullptr;
    unsigned port = 0;
    const char * database = nullptr;
    const char * user = nullptr;
    const char * password = nullptr;
};

struct ArchivedWuRecord;

interface IArchiveSqlConnection : extends IInterface
{
    virtual void ensureSchema() = 0;
    virtual void publish(const ArchivedWuRecord & record) = 0;
};

interface IArchiveSqlBackend
{
    virtual const char * queryName() const = 0;
    virtual IArchiveSqlConnection * connect(const SqlConnectionInfo & info) = 0;
};

interface IArchivedWuPublisher;
extern IArchiveSqlBackend & queryArchiveSqlBackendDuckdb();

interface IPropertyTree;
extern IArchivedWuPublisher * createSqlArchivedWuPublisher(const IPropertyTree * sqlConfig);
