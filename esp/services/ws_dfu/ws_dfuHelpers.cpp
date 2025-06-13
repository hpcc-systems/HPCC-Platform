/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "esp.hpp"
#include "wshelpers.hpp"

#include "ws_dfuHelpers.hpp"

void WsDFUHelpers::appendDFUQueryFilter(const char* name, DFUQFilterType type, const char* value, StringBuffer& filterBuf)
{
    if (isEmptyString(name) || isEmptyString(value))
        return;
    filterBuf.append(type).append(DFUQFilterSeparator).append(name).append(DFUQFilterSeparator).append(value).append(DFUQFilterSeparator);
}

void WsDFUHelpers::appendDFUQueryFilter(const char* name, DFUQFilterType type, const char* value, const char* valueHigh, StringBuffer& filterBuf)
{
    if (isEmptyString(name) || isEmptyString(value))
        return;
    filterBuf.append(type).append(DFUQFilterSeparator).append(name).append(DFUQFilterSeparator).append(value).append(DFUQFilterSeparator);
    filterBuf.append(valueHigh).append(DFUQFilterSeparator);
}

const char* WsDFUHelpers::getPrefixFromLogicalName(const char* logicalName, StringBuffer& prefix)
{
    if (isEmptyString(logicalName))
        return nullptr;

    const char *c=strstr(logicalName, "::");
    if (c)
        prefix.append(c-logicalName, logicalName);
    else
        prefix.append(logicalName);
    return prefix.str();
}

bool WsDFUHelpers::addToLogicalFileList(IPropertyTree& file, const char* nodeGroup, double version, IArrayOf<IEspDFULogicalFile>& logicalFiles)
{
    const char* logicalName = file.queryProp(getDFUQResultFieldName(DFUQResultField::name));
    if (isEmptyString(logicalName))
        return false;

    try
    {
        Owned<IEspDFULogicalFile> lFile = createDFULogicalFile();
        lFile->setName(logicalName);
        lFile->setOwner(file.queryProp(getDFUQResultFieldName(DFUQResultField::owner)));

        StringBuffer buf(file.queryProp(getDFUQResultFieldName(DFUQResultField::timemodified)));
        lFile->setModified(buf.replace('T', ' '));
        lFile->setPrefix(getPrefixFromLogicalName(logicalName, buf.clear()));
        lFile->setDescription(file.queryProp(getDFUQResultFieldName(DFUQResultField::description)));

        if (isEmptyString(nodeGroup))
            nodeGroup = file.queryProp(getDFUQResultFieldName(DFUQResultField::nodegroup));
        if (!isEmptyString(nodeGroup))
        {
            if (version < 1.26)
                lFile->setClusterName(nodeGroup);
            else
                lFile->setNodeGroup(nodeGroup);
        }

        int numSubFiles = 0;
        if (!file.hasProp(getDFUQResultFieldName(DFUQResultField::numsubfiles)))
        {
            lFile->setIsSuperfile(false);
            lFile->setDirectory(file.queryProp(getDFUQResultFieldName(DFUQResultField::directory)));
            lFile->setParts(file.queryProp(getDFUQResultFieldName(DFUQResultField::numparts)));
        }
        else
        {
            lFile->setIsSuperfile(true);
            if (version >= 1.52)
            {
                numSubFiles = file.getPropInt(getDFUQResultFieldName(DFUQResultField::numsubfiles));
                lFile->setNumOfSubfiles(numSubFiles);
            }
        }
        lFile->setBrowseData(numSubFiles > 1 ? false : true); ////Bug 41379 - ViewKeyFile Cannot handle superfile with multiple subfiles

        if (version >= 1.30)
        {
            if (file.hasProp(getDFUQResultFieldName(DFUQResultField::protect)))
                lFile->setIsProtected(true);
            if (file.getPropBool(getDFUQResultFieldName(DFUQResultField::persistent), false))
                lFile->setPersistent(true);
            if (file.hasProp(getDFUQResultFieldName(DFUQResultField::superowners)))
                lFile->setSuperOwners(file.queryProp(getDFUQResultFieldName(DFUQResultField::superowners)));
        }

        __int64 size = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::origsize), 0);
        if (size > 0)
        {
            StringBuffer s;
            lFile->setIntSize(size);
            lFile->setTotalsize(s<<comma(size));
        }

        __int64 records = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::recordcount), 0);
        if (!records)
            records = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::origrecordcount), 0);
        if (!records)
        {
            __int64 recordSize = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::recordsize), 0);
            if(recordSize > 0)
                records = size/recordSize;
        }
        if (records > 0)
        {
            StringBuffer s;
            lFile->setIntRecordCount(records);
            lFile->setRecordCount(s<<comma(records));
        }

        bool isKeyFile = false;
        if (version > 1.13)
        {
            const char* kind = file.queryProp(getDFUQResultFieldName(DFUQResultField::kind));
            if (!isEmptyString(kind))
            {
                if (strieq(kind, "key"))
                    isKeyFile = true;
                if (version >= 1.24)
                    lFile->setContentType(kind);
                else
                    lFile->setIsKeyFile(isKeyFile);
            }
        }

        if (isKeyFile && (version >= 1.41))
        {
            if (isFilePartitionKey(file))
                lFile->setKeyType("Partitioned");
            else if (isFileLocalKey(file))
                lFile->setKeyType("Local");
            else
                lFile->setKeyType("Distributed");
        }

        bool isFileCompressed = file.getPropBool(getDFUQResultFieldName(DFUQResultField::iscompressed));
        if (isFileCompressed)
        {
            if (version >= 1.22)
            {
                if (file.hasProp(getDFUQResultFieldName(DFUQResultField::compressedsize)))
                    lFile->setCompressedFileSize(file.getPropInt64(getDFUQResultFieldName(DFUQResultField::compressedsize)));
                else if (isKeyFile)
                    lFile->setCompressedFileSize(size);
            }
        }
        if (version < 1.22)
            lFile->setIsZipfile(isFileCompressed);
        else
            lFile->setIsCompressed(isFileCompressed);

        if (version >= 1.55)
        {
            StringBuffer accessed(file.queryProp(getDFUQResultFieldName(DFUQResultField::accessed)));
            if (!accessed.isEmpty())
                lFile->setAccessed(accessed.replace('T', ' '));
        }
        if (version >= 1.61)
        {
            if (version < 1.62)
            {
                cost_type cost = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::cost));
                lFile->setCost(cost_type2money(cost));
            }
            else
            {
                cost_type atRestCost = file.getPropInt64(getDFUQResultFieldName(DFUQResultField::atRestCost));
                lFile->setAtRestCost(cost_type2money(atRestCost));
                cost_type accessCost = file .getPropInt64(getDFUQResultFieldName(DFUQResultField::accessCost));
                lFile->setAccessCost(cost_type2money(accessCost));
            }
        }
        if ((version >= 1.63) && (file.hasProp(getDFUQResultFieldName(DFUQResultField::maxSkew))))
        {
            lFile->setMaxSkew(file.getPropInt64(getDFUQResultFieldName(DFUQResultField::maxSkew)));
            lFile->setMinSkew(file.getPropInt64(getDFUQResultFieldName(DFUQResultField::minSkew)));
            lFile->setMaxSkewPart(file.getPropInt64(getDFUQResultFieldName(DFUQResultField::maxSkewPart)));
            lFile->setMinSkewPart(file.getPropInt64(getDFUQResultFieldName(DFUQResultField::minSkewPart)));
        }

        logicalFiles.append(*lFile.getClear());
    }
    catch(IException* e)
    {
        VStringBuffer msg("Failed to retrieve data for logical file %s: ", logicalName);
        int code = e->errorCode();
        e->errorMessage(msg);
        e->Release();
        throw MakeStringException(code, "%s", msg.str());
    }
    return true;
}
