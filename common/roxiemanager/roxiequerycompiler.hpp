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

#ifndef _ROXIEQUERYCOMPILER_HPP__
#define _ROXIEQUERYCOMPILER_HPP__


interface IConstWorkUnit;
interface IWorkUnit;
interface IEmbeddedXslTransformer;
interface IAttributeMetaDataResolver;

#include "jptree.hpp"
#include "esp.hpp"

#include "roxiemanager.hpp"


interface IRoxieQueryCompiler : extends IInterface
{
    virtual IConstWorkUnit *createWorkunit(SCMStringBuffer &wuid, const char *userName, const char *queryAppName) = 0;
    virtual IConstWorkUnit *compileEcl(SCMStringBuffer &wuid, const char *userName, const char *password, IRoxieQueryCompileInfo &compileInfo, IRoxieQueryProcessingInfo &processingInfo, SCMStringBuffer &status) = 0;
};

extern IRoxieQueryCompiler *createRoxieQueryCompiler();

#endif

