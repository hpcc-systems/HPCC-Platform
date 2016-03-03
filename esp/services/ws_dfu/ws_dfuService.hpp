/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _ESPWIZ_WsDfu_HPP__
#define _ESPWIZ_WsDfu_HPP__

#ifdef _WIN32
    #define FILEVIEW_API __declspec(dllimport)
#else
    #define FILEVIEW_API
#endif

#include "ws_dfu_esp.ipp"

#include "fileview.hpp"
#include "fvrelate.hpp"
#include "dadfs.hpp"

class CThorNodeGroup: public CInterface
{
    unsigned timeCached;
    StringAttr groupName;
    unsigned keyhash;
    unsigned nodeCount;
    bool replicateOutputs;

public:
    IMPLEMENT_IINTERFACE;
    CThorNodeGroup(const char* _groupName, unsigned _nodeCount, bool _replicateOutputs)
        : groupName(_groupName), nodeCount(_nodeCount), replicateOutputs(_replicateOutputs)
    {
        keyhash = hashnc((const byte *)groupName.get(),groupName.length(),0);
        timeCached = msTick();
    }

    inline unsigned queryHash() const { return keyhash; }
    inline const char *queryGroupName() const { return groupName.get(); }

    inline unsigned queryNodeCount() const { return nodeCount; };
    inline bool queryReplicateOutputs() const { return replicateOutputs; };
    inline bool queryCanReplicate() const { return replicateOutputs && (nodeCount > 1); };
    inline bool checkTimeout(unsigned timeout) const { return msTick()-timeCached >= timeout; }
};

class CThorNodeGroupCache: public SuperHashTableOf<CThorNodeGroup, const char>
{
    CriticalSection sect;
    CThorNodeGroup* readNodeGroup(const char* _groupName);

public:
    IMPLEMENT_IINTERFACE;

    ~CThorNodeGroupCache() { releaseAll(); }

    inline void onAdd(void *e)
    {
        // not used
    }

    inline void onRemove(void *e)
    {
        CThorNodeGroup *g = (CThorNodeGroup *)e;
        g->Release();
    }

    inline unsigned getHashFromElement(const void *e) const
    {
        return ((CThorNodeGroup *) e)->queryHash();
    }

    inline unsigned getHashFromFindParam(const void *fp) const
    {
        return hashnc((const unsigned char *)fp, strlen((const char *)fp), 0);
    }

    inline const void * getFindParam(const void *e) const
    {
        return ((CThorNodeGroup *) e)->queryGroupName();
    }

    inline bool matchesFindParam(const void * e, const void *fp, unsigned) const
    {
        return (strieq(((CThorNodeGroup *) e)->queryGroupName(), (const char *)fp));
    }

    CThorNodeGroup *lookup(const char* groupName, unsigned timeOutMinutes);
};

class CWsDfuSoapBindingEx : public CWsDfuSoapBinding
{
public:
    CWsDfuSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : CWsDfuSoapBinding(cfg, name, process, llevel){}

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
    }
};


class CWsDfuEx : public CWsDfu
{
private:
    Owned<IXslProcessor> m_xsl;
    Mutex m_superfilemutex;
    unsigned nodeGroupCacheTimeout;
    Owned<CThorNodeGroupCache> thorNodeGroupCache;

public:
    IMPLEMENT_IINTERFACE;
    virtual ~CWsDfuEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    bool onDFUOpenFile(IEspContext &context, IEspDFUSearchRequest &req, IEspDFUSearchResponse &resp);
    bool onDFUSearchNonSubFiles(IEspContext &context, IEspDFUSearchRequest &req, IEspDFUSearchResponse &resp);
    bool onDFUSearch(IEspContext &context, IEspDFUSearchRequest &req, IEspDFUSearchResponse &resp);
    bool onDFUQuery(IEspContext &context, IEspDFUQueryRequest &req, IEspDFUQueryResponse &resp);
    bool onDFUInfo(IEspContext &context, IEspDFUInfoRequest &req, IEspDFUInfoResponse &resp);
    bool onDFUSpace(IEspContext &context, IEspDFUSpaceRequest &req, IEspDFUSpaceResponse &resp);
    bool onDFUDefFile(IEspContext &context,IEspDFUDefFileRequest &req, IEspDFUDefFileResponse &resp);
    bool onDFUArrayAction(IEspContext &context, IEspDFUArrayActionRequest &req, IEspDFUArrayActionResponse &resp);
    bool onAddtoSuperfile(IEspContext &context, IEspAddtoSuperfileRequest &req, IEspAddtoSuperfileResponse &resp);
    bool onDFUFileView(IEspContext &context, IEspDFUFileViewRequest &req, IEspDFUFileViewResponse &resp);
    bool onDFUGetDataColumns(IEspContext &context, IEspDFUGetDataColumnsRequest &req, IEspDFUGetDataColumnsResponse &resp);
    bool onDFUBrowseData(IEspContext &context, IEspDFUBrowseDataRequest &req, IEspDFUBrowseDataResponse &resp);
    bool onDFUSearchData(IEspContext &context, IEspDFUSearchDataRequest &req, IEspDFUSearchDataResponse &resp);
    bool onDFUGetFileMetaData(IEspContext &context, IEspDFUGetFileMetaDataRequest &req, IEspDFUGetFileMetaDataResponse &resp);

    virtual bool onSavexml(IEspContext &context, IEspSavexmlRequest &req, IEspSavexmlResponse &resp);
    virtual bool onAdd(IEspContext &context, IEspAddRequest &req, IEspAddResponse &resp);
    virtual bool onAddRemote(IEspContext &context, IEspAddRemoteRequest &req, IEspAddRemoteResponse &resp);
    virtual bool onSuperfileList(IEspContext &context, IEspSuperfileListRequest &req, IEspSuperfileListResponse &resp);
    virtual bool onSuperfileAction(IEspContext &context, IEspSuperfileActionRequest &req, IEspSuperfileActionResponse &resp);

private:
    const char* getPrefixFromLogicalName(const char* logicalName, StringBuffer& prefix);
    const char* getShortDescription(const char* description, StringBuffer& shortDesc);
    bool addDFUQueryFilter(DFUQResultField *filters, unsigned short &count, MemoryBuffer &buff, const char* value, DFUQResultField name);
    void appendDFUQueryFilter(const char *name, DFUQFilterType type, const char *value, StringBuffer& filterBuf);
    void appendDFUQueryFilter(const char *name, DFUQFilterType type, const char *value, const char *valueHigh, StringBuffer& filterBuf);
    void setFileIterateFilter(unsigned maxFiles, StringBuffer &filterBuf);
    void setFileTypeFilter(const char* fileType, StringBuffer& filterBuf);
    void setFileNameFilter(const char* fname, const char* prefix, StringBuffer &buff);
    void setDFUQueryFilters(IEspDFUQueryRequest& req, StringBuffer& filterBuf);
    void setDFUQuerySortOrder(IEspDFUQueryRequest& req, StringBuffer& sortBy, bool& descending, DFUQResultField* sortOrder);
    bool addToLogicalFileList(IPropertyTree& file, const char* nodeGroup, double version, IArrayOf<IEspDFULogicalFile>& logicalFiles);
    void setDFUQueryResponse(IEspContext &context, unsigned totalFiles, StringBuffer& sortBy, bool descending, unsigned pageStart,
        unsigned pageSize, IEspDFUQueryRequest & req, IEspDFUQueryResponse & resp);
    void getLogicalFileAndDirectory(IEspContext &context, IUserDescriptor* udesc, const char *dirname, IArrayOf<IEspDFULogicalFile>& LogicalFiles, int& numFiles, int& numDirs);
    bool doLogicalFileSearch(IEspContext &context, IUserDescriptor* udesc, IEspDFUQueryRequest & req, IEspDFUQueryResponse & resp);
    void doGetFileDetails(IEspContext &context, IUserDescriptor* udesc, const char *name,const char *cluster,
        const char *description,IEspDFUFileDetail& FileDetails);
    bool createSpaceItemsByDate(IArrayOf<IEspSpaceItem>& SpaceItems, StringBuffer interval, unsigned& yearFrom, 
        unsigned& monthFrom, unsigned& dayFrom, unsigned& yearTo, unsigned& monthTo, unsigned& dayTo);
    bool setSpaceItemByScope(IArrayOf<IEspSpaceItem>& SpaceItems64, const char*scopeName, const char*logicalName, __int64 size);
    bool setSpaceItemByOwner(IArrayOf<IEspSpaceItem>& SpaceItems64, const char *owner, const char *logicalName, __int64 size);
    bool setSpaceItemByDate(IArrayOf<IEspSpaceItem>& SpaceItems, StringBuffer interval, StringBuffer mod, const char*logicalName, __int64 size);
    bool findPositionToAdd(const char *datetime, const __int64 size, const int numNeeded, const unsigned orderType,
                       IArrayOf<IEspDFULogicalFile>& LogicalFiles, int& addToPos, bool& reachLimit);
    __int64 findPositionByParts(const __int64 parts, bool decsend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionBySize(const __int64 size, bool decsend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByRecords(const __int64 records, bool decsend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByName(const char *name, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByOwner(const char *owner, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByNodeGroup(double version, const char *nodeGroup, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByDate(const char *datetime, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByDescription(const char *description, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    bool checkDescription(const char *description, const char *descriptionFilter);
    void getAPageOfSortedLogicalFile(IEspContext &context, IUserDescriptor* udesc, IEspDFUQueryRequest & req, IEspDFUQueryResponse & resp);
    void getDefFile(IUserDescriptor* udesc, const char* FileName,StringBuffer& returnStr);
    void xsltTransformer(const char* xsltPath,StringBuffer& source,StringBuffer& returnStr);
    bool checkFileContent(IEspContext &context, IUserDescriptor* udesc, const char * logicalName, const char * cluster);
    void getRoxieClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress, int& port);
    bool checkRoxieQueryFilesOnDelete(IEspDFUArrayActionRequest &req, StringArray& roxieQueries);
    bool DFUDeleteFiles(IEspContext &context, IEspDFUArrayActionRequest &req, IEspDFUArrayActionResponse &resp);

    void setRootFilter(INewResultSet* result, const char* filterBy, IResultSetFilter* filter, bool disableUppercaseTranslation = true);
    void getMappingColumns(IRelatedBrowseFile * file, bool isPrimary, UnsignedArray& cols);
    void readColumnsForDisplay(StringBuffer& schemaText, StringArray& columnsDisplay, StringArray& columnsDisplayType);
    void mergeSchema(IRelatedBrowseFile * file, StringBuffer& schemaText, StringBuffer schemaText2, 
        StringArray& columnsDisplay, StringArray& columnsDisplayType, StringArray& columnsHide);
    void mergeDataRow(StringBuffer& newRow, int depth, IPropertyTreeIterator* it, StringArray& columnsHide, StringArray& columnsUsed);
    void mergeDataRow(StringBuffer& newRow, StringBuffer dataRow1, StringBuffer dataRow2, StringArray& columnsHide);
    void browseRelatedFileSchema(IRelatedBrowseFile * file, const char* parentName, unsigned depth, StringBuffer& schemaText, 
        StringArray& columnsDisplay, StringArray& columnsDisplayType, StringArray& columnsHide);
    int browseRelatedFileDataSet(double version, IRelatedBrowseFile * file, const char* parentName, unsigned depth, __int64 start, __int64& count, __int64& read, 
                                        StringArray& columnsHide, StringArray& dataSetOutput);
    int GetIndexData(IEspContext &context, bool bSchemaOnly, const char* indexName, const char* parentName, const char* filterBy, __int64 start, 
                                        __int64& count, __int64& read, __int64& total, StringBuffer& message, StringArray& columnLabels, 
                                        StringArray& columnLabelsType, IArrayOf<IEspDFUData>& DataList, bool webDisableUppercaseTranslation);
    bool getUserFilePermission(IEspContext &context, IUserDescriptor* udesc, const char* logicalName, int& permission);
    void parseStringArray(const char *input, StringArray& strarray);
    int superfileAction(IEspContext &context, const char* action, const char* superfile, StringArray& subfiles,
        const char* beforeSubFile, bool existingSuperfile, bool autocreatesuper, bool deleteFile, bool removeSuperfile =  true);
    void getFilePartsOnClusters(IEspContext &context, const char* clusterReq, StringArray& clusters, IDistributedFile* df, IEspDFUFileDetail& FileDetails,
        offset_t& mn, offset_t& mx, offset_t& sum, offset_t& count);
private:
    bool         m_disableUppercaseTranslation;
    StringBuffer m_clusterName;
    StringBuffer m_eclServer;
    StringBuffer daliServers_;
    StringBuffer defaultScope_;
    StringBuffer user_;
    StringBuffer password_;
};


//helper functions
inline const char * splitName(const char * name)
{
    int last = strlen(name)-1;
    for (char * finger=(char*)name+last; finger > name + 1 ; --finger)
    {
        if (*finger == ':' && *(finger-1) == ':')
        {
            assertex(finger<name+last);
            return finger+1;
        }
    }

    return name;
}

#endif //_ESPWIZ_WsDfu_HPP__

