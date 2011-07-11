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

