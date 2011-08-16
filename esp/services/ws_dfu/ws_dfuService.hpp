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

#ifndef _ESPWIZ_WsDfu_HPP__
#define _ESPWIZ_WsDfu_HPP__

#ifdef _WIN32
    #define FILEVIEW_API __declspec(dllimport)
#else
    #define FILEVIEW_API
#endif

#include "ws_dfu_esp.ipp"
#include "ws_dfuView.hpp"
#include "fileview.hpp"
#include "fvrelate.hpp"

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

    virtual bool onSavexml(IEspContext &context, IEspSavexmlRequest &req, IEspSavexmlResponse &resp);
    virtual bool onAdd(IEspContext &context, IEspAddRequest &req, IEspAddResponse &resp);
    virtual bool onAddRemote(IEspContext &context, IEspAddRemoteRequest &req, IEspAddRemoteResponse &resp);
    virtual bool onSuperfileList(IEspContext &context, IEspSuperfileListRequest &req, IEspSuperfileListResponse &resp);
    virtual bool onSuperfileAction(IEspContext &context, IEspSuperfileActionRequest &req, IEspSuperfileActionResponse &resp);
    virtual bool onSuperfileAddRaw(IEspContext &context, IEspSuperfileAddRawRequest &req, IEspSuperfileAddRawResponse &resp);

private:
    void getLogicalFileAndDirectory(IUserDescriptor* udesc, const char *dirname, IArrayOf<IEspDFULogicalFile>& LogicalFiles, int& numFiles, int& numDirs);
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
    __int64 findPositionByCluster(const char *Cluster, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByDate(const char *datetime, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    __int64 findPositionByDescription(const char *description, bool descend, IArrayOf<IEspDFULogicalFile>& LogicalFiles);
    bool checkDescription(const char *description, const char *descriptionFilter);
    void getDefFile(IUserDescriptor* udesc, const char* FileName,StringBuffer& returnStr);
    void xsltTransformer(const char* xsltPath,StringBuffer& source,StringBuffer& returnStr);
    bool onDFUAction(IUserDescriptor* udesc, const char* LogicalFileName,const char* ClusterName,const char* ActionType, bool nodelete, StringBuffer& returnStr);
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

