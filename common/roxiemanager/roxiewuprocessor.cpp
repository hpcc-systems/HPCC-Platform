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

#include <algorithm>
#include "jlib.hpp"
#include "roxiewuprocessor.hpp"
#include "roxiemanager.hpp"
#include "roxiecommlibscm.hpp"
#include "workunit.hpp"
#include "dllserver.hpp"
#include "eclhelper.hpp"
#include "fileview.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"
#include "dfuutil.hpp"
#include "dautils.hpp"
#include "portlist.h"
#include "rmtfile.hpp"
#include "environment.hpp"

typedef MapStringTo<int> DedupeList;

class CRoxieQueryProcessingInfo: public CInterface, implements IRoxieQueryProcessingInfo
{
private:
    
    bool loadDataOnly;
    bool resolveFileInfo;
    bool useLocalDfuFiles;
    bool useRenamedFileInfo;
    bool noForms;
    bool generatePackageFileInfo;

    bool resolveKeyDiffInfo;
    bool copyKeyDiffLocationInfo;

    int layoutTranslationEnabled;
    
    StringBuffer comment;
    StringBuffer packageName;
    StringBuffer dfsDaliIp;
    StringBuffer queryName;
    StringBuffer sourceRoxieClusterName;
    StringBuffer scope;

    IUserDescriptor* userDesc;

public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryProcessingInfo()
    {
        loadDataOnly = false;
        resolveFileInfo = false;
        useLocalDfuFiles = false;
        noForms = false;
        useRenamedFileInfo = false;
        generatePackageFileInfo = false;
        userDesc = NULL;
        layoutTranslationEnabled = RLT_UNKNOWN;
        resolveKeyDiffInfo = true;
        copyKeyDiffLocationInfo = true;
    }

    virtual void setLoadDataOnly(bool val) { loadDataOnly = val; }
    virtual bool getLoadDataOnly() { return loadDataOnly; }
    
    virtual void setResolveFileInfo(bool val) { resolveFileInfo = val; }
    virtual bool getResolveFileInfo() { return resolveFileInfo; }

    virtual void setUseLocalDfuFiles(bool val) { useLocalDfuFiles = val; }
    virtual bool getUseLocalDfuFiles() { return useLocalDfuFiles; }

    virtual void setNoForms(bool val) { noForms = val; }
    virtual bool getNoForms() { return noForms; }

    virtual void setComment(const char *val) { comment.append(val); }
    virtual const char * queryComment() { return ((comment.length()) ? comment.str() : NULL); }

    virtual void setPackageName(const char *val) { packageName.append(val); }
    virtual const char * queryPackageName() { return ((packageName.length()) ? packageName.str() : NULL); }

    virtual void setDfsDaliIp(const char *val) { dfsDaliIp.clear().append(val); }
    virtual const char * queryDfsDaliIp() { return ((dfsDaliIp.length()) ? dfsDaliIp.str() : NULL); }

    virtual void setQueryName(const char *val) { queryName.append(val); }
    virtual const char * queryQueryName() { return ((queryName.length()) ? queryName.str() : NULL); }

    virtual void setSourceRoxieClusterName(const char *val) { sourceRoxieClusterName.append(val); }
    virtual const char * querySourceRoxieClusterName() { return ((sourceRoxieClusterName.length()) ? sourceRoxieClusterName.str() : NULL); }

    virtual void setUseRenamedFileInfo(bool val) { useRenamedFileInfo = val; }
    virtual bool getUseRenamedFileInfo() { return useRenamedFileInfo; }

    virtual void setGeneratePackageFileInfo(bool val) { generatePackageFileInfo = val; }
    virtual bool getGeneratePackageFileInfo() { return generatePackageFileInfo; } 

    virtual void setScope(const char *val) { scope.append(val); }
    virtual const char * queryScope() { return ((scope.length()) ? scope.str() : NULL); }

    virtual void setUserDescriptor(IUserDescriptor *val) { userDesc = val; }
    virtual IUserDescriptor * queryUserDescriptor() { return userDesc; }

    virtual void setLayoutTranslationEnabled(int val) { layoutTranslationEnabled = val; }
    virtual int getLayoutTranslationEnabled() { return layoutTranslationEnabled; }

    virtual void setResolveKeyDiffInfo(bool val) { resolveKeyDiffInfo = val; }
    virtual bool getResolveKeyDiffInfo() { return resolveKeyDiffInfo; }

    virtual void setCopyKeyDiffLocationInfo(bool val) { copyKeyDiffLocationInfo= val; }
    virtual bool getCopyKeyDiffLocationInfo() { return copyKeyDiffLocationInfo; }

};



class CRoxieWuProcessor : public CInterface, implements IRoxieWuProcessor
{
private:
    SocketEndpoint ep;
    StringBuffer roxieClusterName;
    Owned<IPropertyTree> packageInfo;
    StringArray unresolvedFiles;
    Linked<IRoxieCommunicationClient> roxieCommClient;
    Owned<IDFUhelper> dfuHelper;
    int logLevel;

//////////////////////////////////////////////
    // helper methods
    inline static IPropertyTree *addAttribute(IPropertyTree &node, const char * name)
    {
        IPropertyTree * att = createPTree();
        att->setProp("@name", name);
        return node.addPropTree("att", att);
    }

    inline static void addAttribute(IPropertyTree &node, const char * name, const char * value)
    {
        addAttribute(node, name)->setProp("@value", value);
    }

    inline static void addAttributeInt(IPropertyTree &node, const char * name, __int64 value)
    {
        addAttribute(node, name)->setPropInt64("@value", value);
    }

    inline static void addAttributeBool(IPropertyTree &node, const char * name, bool value)
    {
        addAttribute(node, name)->setPropBool("@value", value);
    }

    StringBuffer &addDll(const char* dllName, unsigned crc, StringBuffer &ret)
    {
        if (!dllName || !*dllName)
            throw MakeStringException(ROXIEMANAGER_UNEXPECTION_WU_ERROR, "***ERROR: Unexpected workunit - MISSING DLL NAME when trying to update workunit information for roxie");

        Owned<IDllEntry> dll = queryDllServer().getEntry(dllName);
        Owned<IDllLocation> dllLoc = dll->getBestLocation();
        RemoteFilename rf;
        dllLoc->getDllFilename(rf);
        return rf.getRemotePath(ret);
    }

    void addDllFile(IPropertyTree *xml, const char* name, StringBuffer &fileLocation, const char* version, int codeVersion, unsigned crc, const char* fileType, bool loaddataonly)
    {
        // only 1 part for a dll file
        IPropertyTree *file = createPTree();
        file->setProp("@id", name);
        if (stricmp(fileType, "plugin") == 0)
            file->setProp("@version", version);
        else
        {
            file->setPropInt("@codeVersion", codeVersion);
            file->setPropInt("@crc", crc);  // only interested in crc if NOT plugin
        }

        file->setProp("@mode", "add");
        file->setProp("@type", fileType);
        file->setPropInt("@numparts", 1);
        if (loaddataonly)
            file->setPropBool("@loadDataOnly", loaddataonly);
    
        if (fileLocation.length())
        {
            IPropertyTree *part = createPTree();
            IPropertyTree *location = createPTree();
            location->setProp(NULL, fileLocation);
            part->addPropTree("Loc", location);
            part->setPropInt("@num", 1);
            file->addPropTree("Part_1", part);
        }

        xml->addPropTree("DLL", file);
    }


    static void expandLogicalName(StringBuffer & fullname, const char * logicalName, const char *scope)
    {
        if (logicalName[0]=='~')
            logicalName++;
        else
        {
            if (scope && strlen(scope))  // scope set via workunit
                fullname.append(scope).append("::");
        }
        fullname.append(logicalName);
    }

    void removeForeign(const char *name, StringBuffer &filename, StringBuffer &foreignIP)
    {
        const char *s = strstr(name, "::");
        if (s) 
        {
            StringBuffer str;
            str.append(s-name,name).trim();

            if (stricmp(str.str(), "foreign") == 0)
            {
                // foreign scope - need to strip off the ip and port
                s += 2;  // skip ::

                const char *nm = strstr(s,"::");
                //s = strstr(s,"::");
                if (nm)
                {
                    foreignIP.append(nm-s, s).trim();
                    nm += 2;
                    while (*nm == ' ')
                        nm++;

                    name = nm;
                }
            }
        }
        filename.append(name);

    }

    void removeScope(const char *name, StringBuffer &filename, StringBuffer &foreignIP, bool removeRoxieClusterName, const char *sourceRoxieClusterName)
    {
        if (*name == '~')
        {
            filename.append('~');
            name++;
        }

        const char *s = strstr(name, "::");
        if (s) 
        {
            StringBuffer str;
            str.append(s-name,name).trim();

            if (stricmp(str.str(), "foreign") == 0)
            {
                // foreign scope - need to strip off the ip and port
                s += 2;  // skip ::

                const char *nm = strstr(s,"::");
                if (nm)
                {
                    foreignIP.append(nm-s, s).trim();
                    nm += 2;
                    while (*nm == ' ')
                        nm++;

                    name = nm;
                }
            }
        }
        if (removeRoxieClusterName)
        {
            StringBuffer temp;
            temp.appendf("%s::", roxieClusterName.str());
            if (!strnicmp(temp.str(), name, temp.length()))
                name += temp.length();
        }

        if (sourceRoxieClusterName && *sourceRoxieClusterName)
        {
            StringBuffer temp;
            temp.appendf("%s::", sourceRoxieClusterName);
            if (!strnicmp(temp.str(), name, temp.length()))
                name += temp.length();
        }

        filename.append(name);
    }

    void changeFileClusterName(const char *name, StringBuffer &filename, const char *currentRoxieClusterName, const char *newRoxieClusterName)
    {
        if (*name == '~')
        {
            filename.append('~');
            name++;
        }

        const char *s = strstr(name, "::");
        if (s) 
        {
            StringBuffer str;
            str.append(s-name,name).trim();

            if (stricmp(str.str(), "foreign") == 0)
            {
                s += 2;  // skip ::

                const char *nm = strstr(s,"::");
                //s = strstr(s,"::");
                if (nm)
                {
                    nm += 2;
                    while (*nm == ' ')
                        nm++;

                    name = nm;
                }
            }
        }

        filename.appendf("%s::", newRoxieClusterName);

        if (currentRoxieClusterName && *currentRoxieClusterName)
        {
            StringBuffer temp;
            temp.appendf("%s::", currentRoxieClusterName);
            if (!strnicmp(temp.str(), name, temp.length()))
                name += temp.length();
        }

        filename.append(name);
    }

    void addSubFile(IPropertyTree *xml, const char *fname, StringBuffer &foreignIP, IPropertyTree *sub, bool preload, IRoxieQueryProcessingInfo &processingInfo, bool isPatch, bool isBaseIndex, IPropertyTree *superPackageTree, bool useSourceClusterName, INode *daliNode, bool updateDali)
    {
        StringBuffer fileName;
        StringBuffer fIP;
        removeScope(fname, fileName, fIP, true, processingInfo.querySourceRoxieClusterName());

        if ( (isPatch || isBaseIndex) && (!processingInfo.getResolveKeyDiffInfo() || !processingInfo.getCopyKeyDiffLocationInfo()) )
            return;  // do nothing if key diff is not enabled

        StringBuffer xpath;
        StringBuffer tagname((isPatch) ? "Patch" : (isBaseIndex) ? "BaseIndex" : "File");
        xpath.appendf("%s[@id='%s']", tagname.str(), fileName.str());

        if (xml->hasProp(xpath.str()))
            return; // already added

        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(sub);
        unsigned numParts = fdesc->numParts();
        offset_t file_crc = fdesc->queryProperties().getPropInt64("@checkSum");
        offset_t recordCount = fdesc->queryProperties().getPropInt64("@recordCount", -1);
        offset_t totalSize = fdesc->queryProperties().getPropInt64("@size", -1);
        offset_t formatCrc = fdesc->queryProperties().getPropInt("@formatCrc", 0);

        bool isCompressed = fdesc->isCompressed();

        {
//          MTimeSection timing(NULL, "entering info in dali");
            StringBuffer epStr;
            if (foreignIP.length() == 0)
            {
                if (daliNode)
                {
                    SocketEndpoint ep;
                    ep.set(daliNode->endpoint());
                    ep.getUrlStr(epStr);
                }
            }
            else
                epStr.append(foreignIP.str());

            StringBuffer remoteRoxieClusterName;
            // MORE - don't care about return value from processFileInfoInDali - errors will be checked later
            processFileInfoInDali(xml, numParts, file_crc, recordCount, totalSize, formatCrc, isCompressed, fileName, remoteRoxieClusterName, epStr, processingInfo.queryUserDescriptor(), (isPatch || isBaseIndex) ? false :updateDali);
        }

        if (numParts > 1)
        {
            Owned<IPropertyTreeIterator> patches = sub->getElements("Attr/Patch");
            ForEach(*patches)
            {
                StringBuffer fullBaseIndexName;
                StringBuffer fullPatchName;

                IPropertyTree &patch = patches->query();
                StringBuffer patchName;
                unsigned patch_crc;
                StringBuffer baseIndexName;
                unsigned baseIndex_crc;

            
                patchName.append(patch.queryProp("@name"));
                patch_crc = (unsigned) patch.getPropInt64("@checkSum");

                IPropertyTree *index = patch.queryPropTree("Index");
                if (index)
                {
                    baseIndexName.append(index->queryProp("@name"));
                    baseIndex_crc = (unsigned) index->getPropInt64("@checkSum");
                }
        
                if (baseIndexName.length())
                {
                    if (useSourceClusterName)
                        fullBaseIndexName.appendf("~%s::%s",processingInfo.querySourceRoxieClusterName(), baseIndexName.str());
                    else
                        fullBaseIndexName.appendf("~%s", baseIndexName.str());

                    Owned<IPropertyTree> df = queryDistributedFileDirectory().getFileTree(baseIndexName, daliNode, processingInfo.queryUserDescriptor(), DALI_FILE_LOOKUP_TIMEOUT);
                    if (df)
                        addSubFile(xml, fullBaseIndexName.str(), foreignIP, df, preload, processingInfo, false, true, NULL, useSourceClusterName, daliNode, updateDali);

                }
                if (patchName.length())
                {
                    if (useSourceClusterName)
                        fullPatchName.appendf("~%s::%s", processingInfo.querySourceRoxieClusterName(), patchName.str());
                    else
                        fullPatchName.appendf("~%s", patchName.str());

                    Owned<IPropertyTree> df = queryDistributedFileDirectory().getFileTree(patchName, daliNode, processingInfo.queryUserDescriptor(), DALI_FILE_LOOKUP_TIMEOUT);
                    if (df)
                        addSubFile(xml, fullPatchName.str(), foreignIP, df, preload, processingInfo, true, false, NULL, useSourceClusterName, daliNode, updateDali);

                }
            }
        }

    }

    void processFileInfo1(IPropertyTree *xml, const char *fileName, const char *nodeName, StringBuffer &foreignIP, IPropertyTree *subtree, IPropertyTree *fileNameNode, bool isKey, bool preload, IRoxieQueryProcessingInfo &processingInfo, IPropertyTree *superPackageTree, INode *daliNode, bool useSourceClusterName)
    {
        addSubFile(xml, fileName, foreignIP, subtree, preload, processingInfo, false, false, superPackageTree, useSourceClusterName, daliNode, true);

        StringBuffer fname;
        StringBuffer fIP;
        removeScope(nodeName, fname, fIP, useSourceClusterName, processingInfo.querySourceRoxieClusterName());
        fileNameNode->setProp("@value", fname);
    }

    void addSuperFile(IPropertyTree *xml, const char *superOwnerName, IPropertyTree *subtree, IPropertyTree *fileNameNode, IPropertyTree &node, StringBuffer &subName, StringBuffer &foreignIP, bool isKey, IRoxieQueryProcessingInfo &processingInfo, IPropertyTree *superPackageTree, bool useSourceClusterName, INode *daliNode)
    {
        StringBuffer scopelessSubName;
        StringBuffer fIP;
        removeScope(subName, scopelessSubName, fIP, true, processingInfo.querySourceRoxieClusterName());
    
        if (subtree)
        {
            bool preload = node.getPropBool("att[@name='preload']/@value", false);

            if (foreignIP.length() == 0)
                foreignIP.append(fIP);

            addSubFile(xml, scopelessSubName, foreignIP, subtree, preload, processingInfo, false, false, superPackageTree, useSourceClusterName, daliNode, true);
        }
    }


    bool processAddQueryFileInfo(IPropertyTree *xml, INode *daliNode, IPropertyTree &node, IRoxieQueryProcessingInfo &processingInfo, IPropertyTree *superkeyFileInfo, IPropertyTree *fileNameNode, const char *altRoxieClusterName, StringBuffer &lookupNameInDali, StringBuffer &fullName, bool useSourceClusterName, IPropertyTree *queryPackageTree, StringBuffer &errorStr, int &errorCode)
    {
        StringBuffer xpath;
        StringBuffer completeFileNameInfo(fullName.str());
        IPropertyTree *file = xml->queryBranch(xpath.appendf("%s[@id=\"%s\"]", "File", fullName.str()).str());  // lookup based on non-roxiecluster name
        bool isOpt = isOptDefined(node);
        bool isKey = true;
        if (!file)
        {

            if (superkeyFileInfo)
            {
                // look for it in the superkeyinfo list
                xpath.clear().appendf("SKeyName[@name='%s']", fullName.str()+1);
                file = superkeyFileInfo->queryPropTree(xpath.str());

                if (file)
                {
                    int numsubfiles = file->getPropInt("@numsubfiles");
                    if ((numsubfiles == 0) && (!isOpt))
                    {
                        errorStr.appendf("SuperKey %s has no key parts - specify OPT to enable empty key.", fullName.str());
                        errorCode = ROXIEMANAGER_MISSING_FILE_PARTS;
                        return false;
                    }

                    if (file->hasProp("File"))  // if nested superkey - lookup again - MORE - if needed save nested info differently
                    {
                        Owned<IPropertyTreeIterator> iter = file->getElements("File");
                        ForEach(*iter)
                        {
                            IPropertyTree &subtree = iter->query();
                            StringBuffer subName(subtree.queryProp("@subName"));
                            StringBuffer foreignIP;
                            addSuperFile(xml, fullName, &subtree, fileNameNode, node, subName, foreignIP, isKey, processingInfo, NULL, useSourceClusterName, daliNode);
                        }
                                            
                        return true;
                    }
                }
            }

            Owned<IPropertyTree> df;
            try
            {
                df.setown(queryDistributedFileDirectory().getFileTree(lookupNameInDali.str()+1, daliNode, processingInfo.queryUserDescriptor(), DALI_FILE_LOOKUP_TIMEOUT)); // don't want to pass the ~
            }
            catch(IException *e)
            {
                int errCode = e->errorCode();
                e->errorMessage(errorStr);
                errorStr.append("\n");
                e->Release();  // not fatal;
                if (errCode == 3) // DFS error - this is fatal
                {
                    if (daliNode)
                    {
                        StringBuffer tempErr;
                        daliNode->endpoint().getUrlStr(tempErr);
                        tempErr.append(" : ");
                        errorStr.insert(0, tempErr.str());
                    }
                    throw MakeStringException(ROXIEMANAGER_FILE_PERMISSION_ERR, "%s", errorStr.str());
                }
            }
            catch(...)
            {
                // don't care - not fatal, assume package file
            }

            if (!df)
            {
                // check if DB query and not really a file
                CDfsLogicalFileName logicalname;
                logicalname.set(lookupNameInDali.str()+1);
                if (logicalname.isQuery())
                    return true;

                errorCode = ROXIEMANAGER_IGNORE_EXCEPTION;
                if (unresolvedFiles.find(fullName.str()) == -1)
                {
                    unresolvedFiles.append(fullName.str());
                    errorCode = ROXIEMANAGER_UNRESOLVED_FILE;
                    errorStr.appendf("Warning: File %s could not be resolved, assuming %s to be resolved via package", fullName.str(), isKey ? "SuperKey" : "SuperFile");
                }
                return false;
            }
            else
            {
                // MORE - don't want to always show this log message for each lookupNameInDali - once is enough for all instances (I think)
                DBGLOG("Adding file info for %s", lookupNameInDali.str()+1);

                const char *filetype = df->queryName();

                if (strcmp(filetype,"SuperFile")==0)
                {
                    StringBuffer non_foreignSuperName;
                    StringBuffer foreignIP;
                    removeScope(fullName.str(), non_foreignSuperName, foreignIP, useSourceClusterName, altRoxieClusterName);

                    if (!processingInfo.getResolveFileInfo())  // not interested in the information contained in the superfile
                        return true;

                    unsigned n = df->getPropInt("@numsubfiles");

                    IPropertyTree *temptree = 0;

                    if (superkeyFileInfo)
                    {
                        temptree = superkeyFileInfo->addPropTree("SKeyName", createPTree());
                        temptree->addProp("@name", fullName.str() + 1);
                        temptree->addPropInt("@numsubfiles", n);
                        temptree->addProp("@filetype", filetype);
                    }

                    IPropertyTree *ptree = 0;
                    IPropertyTree *superPackageTree = 0;
                    if (queryPackageTree)
                    {
                        ptree = createPTree("Package");

                        ptree->addProp("@id", non_foreignSuperName.str()+1);
                    
                        IPropertyTree *basePackageTree = createPTree("Base");
                        basePackageTree->addProp("@id", non_foreignSuperName.str()+1);
                        queryPackageTree->addPropTree("Base", basePackageTree);
                    }
                    superPackageTree = createPTree("SuperFile");
                    superPackageTree->addProp("@id", non_foreignSuperName.str());
                    
                    StringBuffer path;
                    if ((n == 0) && (!isOpt))
                    {
                        errorCode = ROXIEMANAGER_MISSING_FILE_PARTS;
                        errorStr.appendf("SuperKey %s has no key parts - specify OPT to enable empty key.", fullName.str());
                        return false;
                    }

                    for (unsigned i=0;i<n;i++)
                    {
                        path.clear().append("SubFile[@num=\"").append(i+1).append("\"]");
                        IPropertyTree *sub = df->queryPropTree(path.str());
                        if (!sub)
                        {
                            errorCode = ROXIEMANAGER_MISSING_FILE_PARTS;
                            errorStr.appendf("%s: Missing sub-file for %s", path.str(), fullName.str());
                            return false;
                        }
                        StringBuffer subName("~");
                        StringBuffer foreignIP;
                        StringBuffer fullSubName(sub->queryProp("@name"));
                        removeScope(fullSubName, subName, foreignIP, useSourceClusterName, altRoxieClusterName);
                        Owned<IPropertyTree> subtree = queryDistributedFileDirectory().getFileTree(fullSubName,daliNode, processingInfo.queryUserDescriptor(), DALI_FILE_LOOKUP_TIMEOUT);

                        if (subtree)
                        {
                            // add it to the superkeyFileInfo tree - so we don't look it up in dali next time
                            if (temptree)
                            {
                                IPropertyTree *t = temptree->addPropTree(subtree->queryName(), LINK(subtree));
                                t->addProp("@subName", subName.str());
                            }
                            
                            if (strcmp(subtree->queryName(),"SuperFile")==0)
                            {
                                Owned<IPropertyTree> newNameNode = createPTree("att");
                                newNameNode->setProp("@value", subName.str());
                                newNameNode->setProp("@superOwner", non_foreignSuperName.str());
                    
                                processAddQueryFileInfo(xml, daliNode, node, processingInfo, superkeyFileInfo, newNameNode, altRoxieClusterName, subName, subName, useSourceClusterName, queryPackageTree, errorStr, errorCode);
                            }
                            else
                                //addSuperFile(xml, non_foreignSuperName, subtree, fileNameNode, node, fullSubName, isKey, processingInfo, superPackageTree, useSourceClusterName, daliNode);
                                addSuperFile(xml, non_foreignSuperName, subtree, fileNameNode, node, subName, foreignIP, isKey, processingInfo, superPackageTree, useSourceClusterName, daliNode);
                        }
                    }
                    if (packageInfo)// && ptree)
                    {
                        if (ptree)
                        {
                            ptree->addPropTree("SuperFile", superPackageTree);
                            packageInfo->addPropTree("Package", ptree);
                        }
                        else
                            packageInfo->addPropTree("SuperFile", superPackageTree);
                    }
                }
                else if (strcmp(filetype,"File")==0)
                {
                    bool preload = node.getPropBool("att[@name='preload']/@value", false);
                    StringBuffer foreignIP;
                    addSubFile(xml, fullName, foreignIP, df, preload, processingInfo, false, false, NULL, useSourceClusterName, daliNode, true);
                }
            }
        }

        return true;
    }


    void addQueryFileInfo(IWorkUnit *wu, INode *daliNode, IPropertyTree *fileNameNode, IPropertyTree *xml, IPropertyTree &node, bool isKey, IRoxieQueryProcessingInfo &processingInfo, IPropertyTree *superkeyFileInfo, IPropertyTree *queryPackageTree)
    {
        // MORE - passing in fileNameNode here seems a bit odd
        const char *filename = fileNameNode->queryProp("@value");
        StringBuffer fullname("~");
        expandLogicalName(fullname, filename, processingInfo.queryScope());
        fullname.toLowerCase();

        StringBuffer errorStr;
        int errorCode;
            
        bool foundit = false;

        if (processingInfo.querySourceRoxieClusterName())
        {
            StringBuffer lookupNameInDali;
            lookupNameInDali.appendf("~%s::%s", processingInfo.querySourceRoxieClusterName(), fullname.str()+1);
            lookupNameInDali.toLowerCase();

            foundit = processAddQueryFileInfo(xml, daliNode, node, processingInfo, superkeyFileInfo, fileNameNode, processingInfo.querySourceRoxieClusterName(), lookupNameInDali, fullname, false, queryPackageTree, errorStr, errorCode);
        }

        if (!foundit)
            foundit = processAddQueryFileInfo(xml, daliNode, node, processingInfo, superkeyFileInfo, fileNameNode, NULL, fullname, fullname, false, queryPackageTree, errorStr, errorCode);
    
        if (!foundit)  // could not find it in any location
            throw MakeStringException(errorCode, "%s", errorStr.str());
    }

    IPropertyTreeIterator *getNodeSubIndexNames(const IPropertyTree &graphNode)
    {
        if (graphNode.hasProp("att[@name='_indexFileName']") || graphNode.hasProp("att[@name='_indexFileName_dynamic']"))
            return graphNode.getElements("att[@name='_indexFileName']");
        else
            return graphNode.getElements("att[@name='_fileName']");
    }

    bool isOptDefined(const IPropertyTree &graphNode)
    {
        if (graphNode.hasProp("att[@name='_isOpt']"))
            return graphNode.getPropBool("att[@name='_isOpt']/@value", false);
        else
            return graphNode.getPropBool("att[@name='_isIndexOpt']/@value", false);
    }

    void setWUException(IException* e, IWorkUnit *wu, SCMStringBuffer &status, bool &warningsOnly)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        setWUException(e->errorCode(), msg, wu, status, warningsOnly);
    }

    void setWUException(int errorCode, StringBuffer &msg, IWorkUnit *wu, SCMStringBuffer &status, bool &warningsOnly)
    {
        Owned<IWUException> we = wu->createException();

        if (errorCode == ROXIEMANAGER_UNRESOLVED_FILE)
        {
            we->setSeverity(ExceptionSeverityWarning);
        }
        else if (errorCode == ROXIEMANAGER_FILE_MISMATCH)
        {
            warningsOnly = false;
            we->setSeverity(ExceptionSeverityError);
        }
        else if (errorCode == ROXIEMANAGER_FILE_PERMISSION_ERR)
        {
            warningsOnly = false;
            we->setSeverity(ExceptionSeverityError);
            we->setExceptionCode(ROXIEMANAGER_FILE_PERMISSION_ERR);
            status.s.appendf("ERROR: ");
        }
        else
        {
            we->setSeverity(ExceptionSeverityError);
        }

        status.s.appendf("%s\n", msg.str());

        we->setExceptionMessage(msg);
        StringBuffer s;
        s.append("roxiequerymanager.cpp");
        we->setExceptionSource(s.str());
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxieWuProcessor(const char *_roxieClusterName, IRoxieCommunicationClient *_roxieCommClient, int _logLevel)
        : roxieClusterName(_roxieClusterName)
        , roxieCommClient(_roxieCommClient)
        , logLevel(_logLevel)
    {
        packageInfo.setown(createPTree("RoxiePackages"));
    }

    bool lookupFileNames(IWorkUnit *wu, IRoxieQueryProcessingInfo &processingInfo, SCMStringBuffer &status)
    {
        dfuHelper.setown(createIDFUhelper());

        SCMStringBuffer scopeStr;
        wu->getScope(scopeStr);
        const char *scope = scopeStr.str();
        processingInfo.setScope(scope);
        Owned<IConstWUGraphIterator> graphs = &wu->getGraphs(GraphTypeActivities); //- MORE - should this return logical graphs??
        
        bool warningsOnly = true;

        Owned<INode> daliNode;
        const char *dfsDaliIp = processingInfo.queryDfsDaliIp();
        if (dfsDaliIp)
        {
            SocketEndpoint ep(dfsDaliIp);
            if (ep.port==0)
                ep.port= DALI_SERVER_PORT;
            daliNode.setown(createINode(ep));
        }

        SCMStringBuffer dllName;
        wu->getDebugValue("queryId", dllName);

        IPropertyTree *xml = createPTree("FILES");
        IPropertyTree *queryPackageTree = NULL;
        
        if (dllName.length() == 0)
        {
            Owned<IConstWUQuery> q = wu->getQuery();
            q->getQueryDllName(dllName);        
        }

        Owned<IPropertyTree> superKeyInfo = createPTree("SuperKeyInfo", ipt_caseInsensitive);
                        
        ForEach(*graphs)
        {
            Owned<IMultiException> me = MakeMultiException();
            Owned <IPropertyTree> xgmml = graphs->query().getXGMMLTree(false);
            try
            {
                Owned<IPropertyTreeIterator> iter = xgmml->getElements(".//node");
                ForEach(*iter)
                {
                    try
                    {
                        IPropertyTree &node = iter->query();
                        IPropertyTree *indexFile = node.queryPropTree("att[@name='_indexFileName']");
                        if (indexFile)
                            addQueryFileInfo(wu, daliNode, indexFile, xml, node, true, processingInfo, superKeyInfo, queryPackageTree);
                        IPropertyTree *dataFile = node.queryPropTree("att[@name='_fileName']");
                        if (dataFile)
                            addQueryFileInfo(wu, daliNode, dataFile, xml, node, false, processingInfo, superKeyInfo, queryPackageTree);
                    }
                    catch(IDFS_Exception *e)
                    {
                        throw e;
                    }
                    catch(IException *e)
                    {
                        if (e->errorCode() == ROXIEMANAGER_IGNORE_EXCEPTION)
                            e->Release();
                        else
                            me->append(*e);
                    }
                }

                StringBuffer msg;
                StringBuffer rel_xpath("MetaFileInfo/Relationships/Relationship");
                addRoxieFileRelationshipsToDali(xml, processingInfo.queryDfsDaliIp(), rel_xpath, true, msg);

                if (me->ordinality())
                    throw me.getClear();
            }
            catch (IMultiException* e)
            {
                //exceptions while compiling ecl may be bundled in a multi exception object and thrown up
                status.s.appendf("==========================NON ECLSERVER ERRORS / WARNINGS==========================================\n"); // add a visual separator for operations
                StringBuffer msg;
                aindex_t count = e->ordinality();

                for (aindex_t i=0; i<count; i++)
                    setWUException(&e->item(i), wu, status, warningsOnly);

                e->Release();
            }
            catch (IException* e)
            {
                status.s.appendf("==========================NON ECLSERVER ERRORS / WARNINGS==========================================\n"); // add a visual separator for operations
                setWUException(e, wu, status, warningsOnly);

                e->Release();
            }

            if (xml->hasProp("File_Error"))
            {
                status.s.appendf("==========================DATA FILE MISMATCH ERRORS==========================================\n"); // add a visual separator for operations
                Owned<IPropertyTreeIterator> err_iter = xml->getElements("File_Error");
                ForEach(*err_iter)
                {
                    IPropertyTree &item = err_iter->query();
                    StringBuffer msg(item.queryProp("@error"));
                    StringBuffer id(item.queryProp("@id"));
                
                    setWUException(ROXIEMANAGER_FILE_MISMATCH, msg, wu, status, warningsOnly);
                }

                wu->setState(WUStateFailed);
                return false;
            }
        }

        if (!warningsOnly)
        {
            wu->setState(WUStateFailed);
            return false;
        }

        return true;
    }

    IPropertyTree *queryPackageInfo()
    {
        return packageInfo;
    }

    void getPeerLocations(StringArray &peerLocations, bool isServerFile, bool isDll, IPropertyTree* topology, int partno)
    {
        unsigned short daliServixPort = getDaliServixPort();
        char directorySep;
        const char *sep;
        if (topology->getPropBool("@linuxOS"))
        {
            directorySep = '/';
            sep = "//";
        }
        else 
        {
            directorySep = '\\';
            sep = "\\\\";
        }

        if (isServerFile)
        {
            // add all Roxie server-side locations here...
            Owned<IPropertyTreeIterator> servers = topology->getElements("./RoxieServerProcess");

            StringArray serverList;
            StringBuffer directory;

            int numServersFound = 0;
            servers->first();
            while(servers->isValid() && numServersFound < 2) // only want 2 roxie server locations retrieved
            {
                IPropertyTree &server = servers->query();
                const char *iptext = server.queryProp("@netAddress");

                if (serverList.find(iptext) != -1)  // MORE - assumes multiple servers on 1 machine will all use same datadirectory - makes no sense not to
                    continue;
                serverList.append(iptext);

                if (isDll)
                {
                    if (directory.length() == 0)  //everyone has the same location for queries
                        directory.append(topology->queryProp("@queryDir"));
                }
                else
                    directory.clear().append(server.queryProp("@dataDirectory"));

                if (directory[directory.length() -1] != directorySep)
                        directory.append(directorySep);

                if (directory[0] != directorySep)
                    directory.insert(0, directorySep);

                StringBuffer location;
                location.append(sep).append(iptext).append(":").append(daliServixPort).append(directory.str());
                peerLocations.append(location);
                numServersFound++;
                servers->next();
            }
        }
        else
        {
            // add all channel locations here...
            Owned<IPropertyTreeIterator> slaves = topology->getElements("./RoxieSlaveProcess");

            int numChannels = topology->getPropInt("@numChannels");

            ForEach(*slaves)
            {
                IPropertyTree &slave = slaves->query();
                int curChannel = slave.getPropInt("@channel");
                int channelNo = ((partno - 1) % numChannels) + 1;
                if (channelNo == curChannel)
                {
                    const char *iptext = slave.queryProp("@netAddress");

                    StringBuffer directory(slave.queryProp("@dataDirectory"));// currently slaves do not put anything in the query directory

                    if (directory[directory.length() -1] != directorySep)
                        directory.append(directorySep);

                    if (directory[0] != directorySep)
                        directory.insert(0, directorySep);

                    directory.replace(':', '$'); // ':' drive RemoteFilename crazy

                    StringBuffer location;
                    location.append(sep).append(iptext).append(":").append(daliServixPort).append(directory.str());
                    peerLocations.append(location);
                }
            }
        }
    }

    void updateLocations(IPropertyTree* location, int partno, bool useRemoteRoxieLocation, bool serverFile, bool isDll, IPropertyTree* topology, const char *name)
    {
        if (useRemoteRoxieLocation)  // only do the work here if the source has the files already copied to it
        {
            StringBuffer elem;
            elem.appendf("Part[@num='%d']", partno);
            IPropertyTree *partlocation = location->queryPropTree(elem.str());
            if (!partlocation)
            {
                elem.clear().appendf("Part_%d", partno);
                partlocation = location->queryPropTree(elem.str());  // legacy format
            }

            if (partlocation)
            {
                Owned<IAttributeIterator> aIter = partlocation->getAttributes();

                location->removeTree(partlocation);
                partlocation = location->addPropTree(elem.str(), createPTree());

                ForEach (*aIter)
                {
                    const char *name = aIter->queryName();
                    const char *value = aIter->queryValue();
                    partlocation->addProp(name, value);
                }

                StringArray peerLocations;
                getPeerLocations(peerLocations, serverFile, isDll, topology, partno);
                ForEachItemIn(idx, peerLocations)
                {
                    StringBuffer filelocation(peerLocations.item(idx));

                    IPropertyTree *t = partlocation->addPropTree("Loc", createPTree());
                    if (isDll)
                    {
                        if (name)
                            filelocation.append(name);
                        t->setProp(NULL, filelocation.str());
                    }
                    else
                        t->setProp("@path", filelocation.str());
                }
            }
        }
    }


//////////////////////////////////////////////////////////////////////////////////////
    bool addRoxieFileRelationshipsToDali(IPropertyTree *xml, const char *lookupDaliIp, const char *xpath, bool storeResults, StringBuffer &msg)
    {
        if (!xml)
            return false;

        bool retval = true;

        if (!dfuHelper)
            dfuHelper.setown(createIDFUhelper());

        Owned<IPropertyTreeIterator> iter = xml->getElements(xpath);
        ForEach(*iter)
        {
            IPropertyTree &item = iter->query();
            const char *primary = item.queryProp("@primaryName");
            const char *secondary = item.queryProp("@secondaryName");
            const char *lPrimary = item.queryProp("@localPrimaryName");
            const char *lSecondary = item.queryProp("@localSecondaryName");

            try
            {
                if (primary && *primary && secondary && *secondary && lPrimary && *lPrimary && lSecondary && *lSecondary)
                {
                    StringArray srcfiles; 
                    srcfiles.append(primary);
                    srcfiles.append(secondary);
                    StringArray dstfiles; 
                    dstfiles.append(lPrimary);
                    dstfiles.append(lSecondary);

                    Owned<IPropertyTree> tree;

                    if (storeResults)
                        tree.setown(createPTree());

                    dfuHelper->cloneFileRelationships(lookupDaliIp,// where src relationships are retrieved from (can be NULL for local)
                                                      srcfiles,     // file names on source
                                                      dstfiles,     // corresponding filenames on dest (must exist otherwise ignored)
                                                      tree  // MORE - add a tree ptr here and process....
                                                     );

                    if (tree)
                    {
                        Owned<IPropertyTreeIterator> tree_iter = tree->getElements("Relationship");
                        ForEach(*tree_iter)
                        {
                            IPropertyTree &item = tree_iter->query();
                            const char *queryName = item.queryName();


                            IPropertyTree *primTree = NULL;
                            IPropertyTree *secTree = NULL;
                    
                            StringBuffer lookupName;
                            if (*primary == '~')
                                lookupName.append(primary);
                            else
                                lookupName.appendf("~%s", primary);
                        
                            StringBuffer xpath;
                            xpath.appendf("MetaFileInfo/File[@logicalId='%s']", lookupName.str());
                            primTree = xml->queryPropTree(xpath.str());

                            if (primTree)
                            {
                                IPropertyTree *relTree = primTree->queryPropTree("AllRelationships");
                                if (!relTree)
                                {
                                    relTree = primTree->addPropTree("AllRelationships", createPTree());
                                    relTree->addProp("@origClusterName", roxieClusterName.str());
                                }
                                if (relTree)
                                    relTree->addPropTree(queryName, LINK(&item));
                            }

                            if (*secondary == '~')
                                lookupName.clear().append(secondary);
                            else
                                lookupName.clear().appendf("~%s", secondary);

                            xpath.clear().appendf("MetaFileInfo/File[@logicalId='%s']", lookupName.str());
                            secTree = xml->queryPropTree(xpath.str());

                            if (secTree)
                            {
                                IPropertyTree *relTree = secTree->queryPropTree("AllRelationships");
                                if (!relTree)
                                {
                                    relTree = secTree->addPropTree("AllRelationships", createPTree());
                                    relTree->addProp("@origClusterName", roxieClusterName.str());
                                }

                                if (relTree)
                                    relTree->addPropTree(queryName, LINK(&item));
                            }
                        }
                    }
                }
            }
            catch(IException *e)
            {
                e->errorMessage(msg);
                e->Release();  // report the error later if needed
                retval = false;
            }
        }

        return retval;
    }

////////////////////////////////////////////////////////////////////////////////////////
    void lookupRelationships(IPropertyTree *relationshipTree, const char *primaryName, const char *secondaryName, const char *remoteRoxieClusterName, const char *lookupDaliIp)
    {
        Owned<IFileRelationshipIterator> iter = queryDistributedFileDirectory().lookupFileRelationships(
                        primaryName, secondaryName, NULL, NULL, NULL, NULL, NULL, lookupDaliIp);

        // need to build a list of file relationship pairs - in the future, may need all info - depends on roxie xref.
        ForEach(*iter) 
        {
            IFileRelationship &rel=iter->query();
            const char* primary = rel.queryPrimaryFilename();
            const char* secondary = rel.querySecondaryFilename();

            if (primary && *primary && secondary && *secondary)
            {
                StringBuffer foreignIP;
                StringBuffer primaryName;
                StringBuffer secondaryName;
                removeForeign(primary, primaryName, foreignIP);
                removeForeign(secondary, secondaryName, foreignIP);

                IPropertyTree *tree = relationshipTree->addPropTree("Relationship", createPTree());
                tree->addProp("@primaryName", primaryName);
                tree->addProp("@secondaryName", secondaryName);

                StringBuffer localPrimaryName;
                StringBuffer localSecondaryName;

                changeFileClusterName(primary, localPrimaryName, remoteRoxieClusterName, roxieClusterName);
                changeFileClusterName(secondary, localSecondaryName, remoteRoxieClusterName, roxieClusterName);

                tree->addProp("@localPrimaryName", localPrimaryName.str());
                tree->addProp("@localSecondaryName", localSecondaryName.str());
            }
        }
    }


////////////////////////////////////////////////////////////////////////////////////////
    bool addRoxieFileInfoToDali(const char *src_filename, const char *dest_filename, const char *remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc, IPropertyTree *xml, StringBuffer &msg)
    {
        bool retval = true;
        try
        {
            bool sharedDali = false;  // MORE - need to know if shared dali or not, will use it later...
            if (dfuHelper)
            {
                Owned<INode> daliNode;
                if (lookupDaliIp && *lookupDaliIp)
                {
                    SocketEndpoint ep(lookupDaliIp);
                    if (ep.port==0)
                        ep.port= DALI_SERVER_PORT;
                    daliNode.setown(createINode(ep));

                    if (queryCoven().inCoven(daliNode))
                        sharedDali = true;
            }

            bool copyFiles = true;
            StringBuffer xpath;
            xpath.appendf("Software/RoxieCluster[@name='%s']", roxieClusterName.str());

            Owned<IDistributedFile> dst = queryDistributedFileDirectory().lookup(src_filename, userdesc, true);
            if (dst)
            {
                if (dst->findCluster(roxieClusterName.str()) != NotFound)
                    return true; // file already known for this cluster
            }

            Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
            Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
            Owned<IPropertyTree> pRoot = &environment->getPTree();

            IPropertyTree* pCluster = pRoot->queryPropTree( xpath.str() );
            if (pCluster)
                copyFiles = pCluster->getPropBool("@copyResources");

            if (!copyFiles)
                return true;  // don't copy files, so don't add - return true since no error occurred
//              if (logLevel > 9)
//                  DBGLOG("Adding source file %s from dali %s to destination file %s", src_filename, lookupDaliIp, dest_filename);
                dfuHelper->createSingleFileClone(src_filename,              // src LFN (can be foreign and can't be super)
                                                 src_filename,              // dst LFN 
                                                 roxieClusterName.str(),    // group name of roxie cluster 
                                                 DFUcpdm_c_replicated_by_d, // how the nodes are mapped
                                                 true,                      // repeat last part on all nodes if key
                                                 NULL,                      // servers cluster (for just tlk)
                                                 userdesc,                  // user desc for local dali
                                                 lookupDaliIp,              // can be omitted if srcname foreign or local
                                                 NULL,                      // user desc for foreign dali if needed
                                                 false,                     // overwrite
                                                 copyFiles                  // copy file ???
                                                );
            }
            else
                retval = false;
        }
        catch(IException *e)
        {
            e->errorMessage(msg);
            e->Release();  // report the error later if needed
            retval = false;
        }
        catch(...)
        {
            retval = false;
        }

        return retval;
    }
//////////////////////////////////////////////////////////

    bool retrieveMetaFileInfo(const char *src_filename, const char *dest_filename, const char *remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc, IPropertyTree *xml, StringBuffer &msg)
    {
        bool retval = true;
        StringBuffer buf;
        try
        {
            if (dfuHelper)
                dfuHelper->getFileXML(dest_filename, buf, userdesc);

            if (xml)
            {
                IPropertyTree *metaTree = xml->queryPropTree("MetaFileInfo");
                if (!metaTree)
                    metaTree = xml->addPropTree("MetaFileInfo", createPTree());

                IPropertyTree *relationshipTree = metaTree->queryPropTree("Relationships");
                if (!relationshipTree)
                    relationshipTree = metaTree->addPropTree("Relationships", createPTree());

                if (buf.length() == 0)
                    throw MakeStringException(ROXIEMANAGER_DALI_LOOKUP_ERROR, "No meta file information found.");

                IPropertyTree *tree = metaTree->addPropTree("File", createPTreeFromXMLString(buf.str()));
                if (!tree)
                    throw MakeStringException(ROXIEMANAGER_DALI_LOOKUP_ERROR, "Could not parse information");
                StringBuffer id(dest_filename);
                StringBuffer sep;
                sep.append(PATHSEPCHAR);
                id.replaceString("::", sep.str());

                tree->addProp("@id", id.str());

                if (dest_filename[0] != '~')
                {
                    StringBuffer temp;
                    temp.appendf("~%s", src_filename);
                    tree->addProp("@logicalId", temp.str());  // dali @id == roxie @logicalId - roxie already uses @id and don't want to change it at this time
                }
                else
                    tree->addProp("@logicalId", src_filename);

                lookupRelationships(relationshipTree, src_filename, NULL, remoteRoxieClusterName, lookupDaliIp);
                lookupRelationships(relationshipTree, NULL, src_filename, remoteRoxieClusterName, lookupDaliIp);
            }
        }
        catch(IException *e)
        {
            e->errorMessage(msg);
            e->Release();  // report the error later if needed
        }
        catch(...)
        {
        }
        
        return retval;
    }


    bool processAddRoxieFileInfoToDali(const char *src_filename, const char *dest_filename, const char *lookupDaliIp, IUserDescriptor *userdesc, StringBuffer &msg)
    {
        if (!dfuHelper)
            dfuHelper.setown(createIDFUhelper());

        bool ret = addRoxieFileInfoToDali(src_filename, dest_filename, NULL, lookupDaliIp, userdesc, NULL, msg);
        retrieveMetaFileInfo(src_filename, dest_filename, NULL, lookupDaliIp, userdesc, NULL, msg);
        return ret;
    }

    bool processAddRoxieFileRelationshipsToDali(const char *src_filename, const char *remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc, StringBuffer &msg)
    {
        if (!dfuHelper)
            dfuHelper.setown(createIDFUhelper());

        Owned<IPropertyTree> tree = createPTree("Relationships");
        IPropertyTree *relationshipTree = tree->addPropTree("Relationships", createPTree());

        lookupRelationships(relationshipTree, src_filename, NULL, remoteRoxieClusterName, lookupDaliIp);
        lookupRelationships(relationshipTree, NULL, src_filename, remoteRoxieClusterName, lookupDaliIp);

        return addRoxieFileRelationshipsToDali(tree, lookupDaliIp, "Relationships/Relationship", false, msg);
    }
    

    bool processFileInfoInDali(IPropertyTree *xml, unsigned numParts, offset_t file_crc, offset_t recordCount, offset_t totalSize, offset_t formatCrc, bool isCompressed, StringBuffer &fname, StringBuffer &remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc, bool updateDali)
    {
        // MORE - what if anything should we be doing with isCompressed here?
        bool retval = true;

        StringBuffer src_filename;
        if (fname.length() && (fname.charAt(0) == '~'))
            src_filename.append(fname.str() + 1);
        else
            src_filename.append(fname.str());

        StringBuffer dest_filename;
        dest_filename.appendf("%s::%s", roxieClusterName.str(), src_filename.str());

        StringBuffer msg;
        if (dfuHelper && updateDali)
        {
            // load remote queries - always use the cluster name when looking up source in dali
            if (remoteRoxieClusterName.length())
            {
                StringBuffer temp;
                temp.appendf("%s::", remoteRoxieClusterName.str());
                src_filename.insert(0, temp.str());
            }
        
            retval = addRoxieFileInfoToDali(src_filename, dest_filename, remoteRoxieClusterName, lookupDaliIp, userdesc, xml, msg);
        }

        if (!retval)  // didn't add it so check it since it was already there
        {
            retval = validateDataFileInfo(xml, numParts, file_crc, recordCount, totalSize, formatCrc, dest_filename, remoteRoxieClusterName, lookupDaliIp, userdesc);
            if (retval)
            {
                updateDataMetaMappingInfo(src_filename, dest_filename, lookupDaliIp, userdesc);
            }
        }

        if (retval && dfuHelper && updateDali)
            retrieveMetaFileInfo(src_filename, dest_filename, remoteRoxieClusterName, lookupDaliIp, userdesc, xml, msg);

        return retval;
    }


    void updateDataMetaMappingInfo(const char *src_filename, const char *dest_filename, const char *lookupDaliIp, IUserDescriptor *userdesc)
    {
        Owned<INode> daliNode;
        if (lookupDaliIp && *lookupDaliIp)
        {
            SocketEndpoint ep(lookupDaliIp);
            if (ep.port==0)
                ep.port= DALI_SERVER_PORT;
            daliNode.setown(createINode(ep));
        }

        Owned<IPropertyTree> df = queryDistributedFileDirectory().getFileTree(src_filename, daliNode, userdesc, DALI_FILE_LOOKUP_TIMEOUT);
        if (df)
        {
            StringBuffer mapping(df->queryProp("Attr/@columnMapping"));
            if (mapping.length())
            {
                Owned<IDistributedFile> dst = queryDistributedFileDirectory().lookup(dest_filename, userdesc, true);
                if (dst)
                    dst->setColumnMapping(mapping);
            }
        }
    }

    bool validateDataFileInfo(IPropertyTree *xml, unsigned numParts, offset_t file_crc, offset_t recordCount, offset_t totalSize, offset_t formatCrc, StringBuffer &dest_filename, StringBuffer &remoteRoxieClusterName, const char *lookupDaliIp, IUserDescriptor *userdesc)
    {
        bool retval = true;
        Owned<IPropertyTree> df = queryDistributedFileDirectory().getFileTree(dest_filename.str(), NULL, userdesc, DALI_FILE_LOOKUP_TIMEOUT);

        if (df)
        {
            IPropertyTree *attr = df->queryPropTree("Attr");
            if (attr)
            {
                unsigned local_numParts = df->getPropInt("@numparts");
                offset_t local_file_crc = attr->getPropInt64("@checkSum");
                offset_t local_recordCount = attr->getPropInt64("@recordCount", -1);
                offset_t local_totalSize = attr->getPropInt64("@size", -1);
                offset_t local_formatCrc = attr->getPropInt64("@formatCrc", 0);

                if ( (local_numParts == numParts) && (local_file_crc == file_crc) && 
                     (local_recordCount == recordCount) && (local_totalSize == totalSize) &&
                     (local_formatCrc == formatCrc))
                     int ii =0;
                else
                {
                    StringBuffer errMsg;
                    errMsg.appendf("Another version of %s already exists in dali", dest_filename.str());
                    errMsg.appendf("  numParts = %d   %d", local_numParts, numParts);
                    errMsg.appendf("  fileCrc = %"I64F"x   %"I64F"x", local_file_crc, file_crc);
                    errMsg.appendf("  recordCount = %"I64F"x   %"I64F"x", local_recordCount, recordCount);
                    errMsg.appendf("  totalSize = %"I64F"x   %"I64F"x", local_totalSize, totalSize);
                    errMsg.appendf("  formatCrc = %"I64F"x   %"I64F"x", local_formatCrc, formatCrc);

                    DBGLOG("ERROR!!!! = \n%s", errMsg.str());

                    IPropertyTree *err = createPTree();
                    err->setProp("@error", errMsg.str());
                    err->setProp("@id", dest_filename.str());
                    xml->addPropTree("File_Error", err);
                    retval = false;
                }
            }
        }

        return retval;
    }

////////////////////////////////////////////////////////////////////////////////////////

    void retrieveFileNames(IPropertyTree *xml, IPropertyTree *remoteQuery, IPropertyTree *remoteState, IPropertyTree *remoteTopology, bool copyFileLocationInfo, const char *lookupDaliIp, const char *user, const char *password)
    {
        xml->addPropTree("Query", LINK(remoteQuery));
        
        dfuHelper.setown(createIDFUhelper());

        bool useRemoteRoxieLocation = remoteTopology->getPropBool("@copyResources", false);
        StringBuffer remoteRoxieClusterName(remoteTopology->queryProp("@name"));

        Owned<IMultiException> me = MakeMultiException();
        try
        {
            // lets get the dll file information for both query and plugin dlls' - assume there is only 1 part
            const char* dllName = remoteQuery->queryProp("@dll");
            StringBuffer xpath;
            xpath.appendf("DLL[@id='%s']", dllName);
            IPropertyTree *local = remoteState->queryPropTree(xpath.str());
            if (local)
            {
                if (remoteRoxieClusterName.length())
                    local->setProp("@remoteCluster", remoteRoxieClusterName); 
                updateLocations(local, 1, useRemoteRoxieLocation, true, true, remoteTopology, dllName);
                xml->addPropTree("DLL", createPTreeFromIPT(local));
            }

            Owned<IPropertyTreeIterator> plugins = remoteQuery->getElements("Plugin");
            ForEach(*plugins)
            {
                IPropertyTree &node = plugins->query();
                const char *pluginName = node.queryProp("@dll");
                xpath.clear().appendf("DLL[@id='%s']", pluginName);
                IPropertyTree *local = remoteState->queryPropTree(xpath.str());
                if (local)
                {
                    if (remoteRoxieClusterName.length())
                        local->setProp("@remoteCluster", remoteRoxieClusterName); 

                    updateLocations(local, 1, useRemoteRoxieLocation, true, true, remoteTopology, pluginName);
                    xml->addPropTree("DLL", createPTreeFromIPT(local));
                }
            }
            if (!copyFileLocationInfo)
            {
                // need to get rid of all Key and File definitions - there shouldn't be any, but lets make sure...
                Owned<IPropertyTreeIterator> keys = remoteQuery->getElements("Key");
                ForEach(*keys)
                    remoteQuery->removeTree(&keys->query());

                Owned<IPropertyTreeIterator> files = remoteQuery->getElements("File");
                ForEach(*files)
                    remoteQuery->removeTree(&files->query());

                IPropertyTree *metaTree = remoteQuery->queryPropTree("MetaFileInfo");
                if (metaTree)
                    remoteQuery->removeTree(metaTree);
            }
        }
        catch(IException *e)
        {
            me->append(*e);
        }

        Owned<IPropertyTreeIterator> graphs = remoteQuery->getElements("Graph");

        // temp storage arrays to make sure we only add names once
        StringArray fileList;
        StringArray indexList;

        ForEach(*graphs)
        {
            Owned<IPropertyTreeIterator> iter = graphs->query().getElements(".//node");
    
            ForEach(*iter)
            {
                try
                {
                    IPropertyTree &node = iter->query();
                    if (node.hasProp("att[@name='_roxieDataActivity']"))
                    {
                        const char *fname = node.queryProp("att[@name=\"_fileName\"]/@value");
                        if (fname)
                        {
                            if (fileList.find(fname) == -1)
                            {
                                fileList.append(fname);
                                // lets find it in the "state" tree
                                DBGLOG("retrieving remote file info for %s", fname);
                                StringBuffer xpath;
                                xpath.appendf("File[@id='%s']", fname);
                                IPropertyTree *local = remoteState->queryPropTree(xpath.str());
                                if (local)
                                {
                                    if (remoteRoxieClusterName.length())
                                        local->setProp("@remoteCluster", remoteRoxieClusterName); 

                                    int numparts = local->getPropInt("@numparts");
                                    for (int i = 1; i <= numparts; i++)
                                        updateLocations(local, i, useRemoteRoxieLocation, false, false, remoteTopology, NULL);
                                        //updateLocations(local, i, useRemoteRoxieLocation, (i == numparts) ? true : false, false, topology, NULL);

                                    xml->addPropTree("File", createPTreeFromIPT(local));
                                }
                            }
                        }

                        if (node.hasProp("att[@name='_indexFileName']"))
                        {
                            // need to check all indexfiles not associated with a superowner
                            Owned<IPropertyTreeIterator> indexfile_iter = node.getElements("att[@name='_indexFileName']");
                            ForEach(*indexfile_iter)
                            {
                                IPropertyTree &indexfile_node = indexfile_iter->query();
                                fname = indexfile_node.queryProp("@value");
                            
                                if (fname)
                                {
                                    if (fileList.find(fname) == -1)
                                    {
                                        fileList.append(fname);
                                        // lets find it in the "state" tree
                                        DBGLOG("retrieving remote file info for %s", fname);
                                        StringBuffer xpath;
                                        xpath.appendf("Key[@id='%s']", fname);
                                        IPropertyTree *local = remoteState->queryPropTree(xpath.str());
                                        if (local)
                                        {
                                            if (remoteRoxieClusterName.length())
                                                local->setProp("@remoteCluster", remoteRoxieClusterName); 

                                            int numparts = local->getPropInt("@numparts");
                                            for (int i = 1; i <= numparts; i++)
                                                updateLocations(local, i, useRemoteRoxieLocation, (i == numparts) ? true : false, false, remoteTopology, NULL);

                                            xml->addPropTree("Key", createPTreeFromIPT(local));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                catch(IException *e)
                {
                    me->append(*e);
                }
            }
        }
    }

    void retrieveFileNames(IPropertyTree *remoteQuery, IPropertyTree *remoteTopology, bool copyFileLocationInfo, const char *lookupDaliIp, const char *user, const char *password)
    {
        Owned<IMultiException> me = MakeMultiException();

        dfuHelper.setown(createIDFUhelper());

        bool useRemoteRoxieLocation = remoteTopology->getPropBool("@copyResources", false);
        StringBuffer remoteRoxieClusterName(remoteTopology->queryProp("@name"));
        StringArray filesToDelete;

        Owned<IUserDescriptor> userdesc;
        if (user && *user && *password && *password)
        {
            userdesc.setown(createUserDescriptor());
            userdesc->set(user, password);
        }

        IPropertyTree *metaTreeInfo = remoteQuery->queryPropTree("MetaFileInfo");  // always get rid of remote meta info here, will add local later
        if (metaTreeInfo)
            remoteQuery->removeTree(metaTreeInfo);

        Owned<IPropertyTreeIterator> p = remoteQuery->getElements("*");
        ForEach(*p)
        {
            IPropertyTree *item = &p->query();
            if (remoteRoxieClusterName.length())
                item->setProp("@remoteCluster", remoteRoxieClusterName); 

            StringBuffer id(item->queryProp("@id"));

            const char *tag = item->queryName();
            if (stricmp(tag, "DLL") == 0)
            {
                try
                {
                    // assume there is only 1 part
                    const char *type = item->queryProp("@type");
                    if (!type || stricmp(type, "plugin"))  // if "plugin" we don't need location information
                        updateLocations(item, 1, useRemoteRoxieLocation, true, true, remoteTopology, id);
                }
                catch(IException *e)
                {
                    me->append(*e);
                }
            }
            else if (stricmp(tag, "Key") == 0)
            {
                if (!copyFileLocationInfo)
                {
                    StringBuffer xpath;
                    xpath.appendf("Key[@id='%s']", id.str());
                    filesToDelete.append(xpath.str());
                }
                else
                {
                    DBGLOG("retrieving remote key info for %s", id.str());
                    unsigned numParts = item->getPropInt("@numparts");
                    offset_t file_crc = item->getPropInt64("@crc");
                    offset_t recordCount = item->getPropInt64("@recordCount", -1);
                    offset_t totalSize = item->getPropInt64("@size", -1);
                    offset_t formatCrc = item->getPropInt64("@formatCrc");

                    if (processFileInfoInDali(remoteQuery, numParts, file_crc, recordCount, totalSize, formatCrc, false, id, remoteRoxieClusterName, lookupDaliIp, userdesc, true))
                    {
                        int numparts = item->getPropInt("@numparts");
                        for (int i = 1; i <= numparts; i++)
                            updateLocations(item, i, useRemoteRoxieLocation, (i == numparts) ? true : false, false, remoteTopology, NULL);
                    }
                }
            }
            else if (stricmp(tag, "File") == 0)
            {
                if (!copyFileLocationInfo)
                {
                    StringBuffer xpath;
                    xpath.appendf("Key[@id='%s']", id.str());
                    filesToDelete.append(xpath.str());
                }
                else
                {
                    DBGLOG("retrieving remote file info for %s", id.str());

                    bool isCompressed = item->getPropBool("@isCompressed");
                    unsigned numParts = item->getPropInt("@numparts");
                    offset_t file_crc = item->getPropInt64("@crc");
                    offset_t recordCount = item->getPropInt64("@recordCount", -1);
                    offset_t totalSize = item->getPropInt64("@size", -1);
                    offset_t formatCrc = item->getPropInt64("@formatCrc");

                    if (processFileInfoInDali(remoteQuery, numParts, file_crc, recordCount, totalSize, formatCrc, isCompressed, id, remoteRoxieClusterName, lookupDaliIp, userdesc, true))
                    {
                        int numparts = item->getPropInt("@numparts");
                        for (int i = 1; i <= numparts; i++)
                            updateLocations(item, i, useRemoteRoxieLocation, false, false, remoteTopology, NULL);
                    }
                }
            }
        }

        StringBuffer msg;
        StringBuffer rel_xpath("MetaFileInfo/Relationships/Relationship");
        addRoxieFileRelationshipsToDali(remoteQuery, lookupDaliIp, rel_xpath, true, msg);

        ForEachItemIn(idx, filesToDelete)
        {
            StringBuffer xpath(filesToDelete.item(idx));
            remoteQuery->removeProp(xpath.str());
        }
    }
};


IRoxieQueryProcessingInfo *createRoxieQueryProcessingInfo()
{
    return new CRoxieQueryProcessingInfo();
}


IRoxieWuProcessor *createRoxieWuProcessor(const char *_roxieClusterName, IRoxieCommunicationClient *_roxieCommClient, int logLevel)
{
    return new CRoxieWuProcessor(_roxieClusterName, _roxieCommClient, logLevel);
}
