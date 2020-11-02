#ifndef WSDFUSERVICE_HPP__
#define WSDFUSERVICE_HPP__

#include "jlib.hpp"
#include "WsDfuServiceBase.hpp"

using namespace std;


class WsDfuService : public WsDfuServiceBase
{
public:
        virtual AddResponse* Add(EsdlContext* context, AddRequest* request);
    virtual AddRemoteResponse* AddRemote(EsdlContext* context, AddRemoteRequest* request);
    virtual AddtoSuperfileResponse* AddtoSuperfile(EsdlContext* context, AddtoSuperfileRequest* request);
    virtual DFUArrayActionResponse* DFUArrayAction(EsdlContext* context, DFUArrayActionRequest* request);
    virtual DFUBrowseDataResponse* DFUBrowseData(EsdlContext* context, DFUBrowseDataRequest* request);
    virtual DFUDefFileResponse* DFUDefFile(EsdlContext* context, DFUDefFileRequest* request);
    virtual DFUFileAccessResponse* DFUFileAccess(EsdlContext* context, DFUFileAccessRequest* request);
    virtual DFUFileAccessResponse* DFUFileAccessV2(EsdlContext* context, DFUFileAccessV2Request* request);
    virtual DFUFileCreateResponse* DFUFileCreate(EsdlContext* context, DFUFileCreateRequest* request);
    virtual DFUFileCreateResponse* DFUFileCreateV2(EsdlContext* context, DFUFileCreateV2Request* request);
    virtual DFUFilePublishResponse* DFUFilePublish(EsdlContext* context, DFUFilePublishRequest* request);
    virtual DFUFileViewResponse* DFUFileView(EsdlContext* context, DFUFileViewRequest* request);
    virtual DFUGetDataColumnsResponse* DFUGetDataColumns(EsdlContext* context, DFUGetDataColumnsRequest* request);
    virtual DFUGetFileMetaDataResponse* DFUGetFileMetaData(EsdlContext* context, DFUGetFileMetaDataRequest* request);
    virtual DFUInfoResponse* DFUInfo(EsdlContext* context, DFUInfoRequest* request);
    virtual DFUQueryResponse* DFUQuery(EsdlContext* context, DFUQueryRequest* request);
    virtual DFURecordTypeInfoResponse* DFURecordTypeInfo(EsdlContext* context, DFURecordTypeInfoRequest* request);
    virtual DFUSearchResponse* DFUSearch(EsdlContext* context, DFUSearchRequest* request);
    virtual DFUSearchDataResponse* DFUSearchData(EsdlContext* context, DFUSearchDataRequest* request);
    virtual DFUSpaceResponse* DFUSpace(EsdlContext* context, DFUSpaceRequest* request);
    virtual EclRecordTypeInfoResponse* EclRecordTypeInfo(EsdlContext* context, EclRecordTypeInfoRequest* request);
    virtual EraseHistoryResponse* EraseHistory(EsdlContext* context, EraseHistoryRequest* request);
    virtual ListHistoryResponse* ListHistory(EsdlContext* context, ListHistoryRequest* request);
    virtual WsDfuPingResponse* Ping(EsdlContext* context, WsDfuPingRequest* request);
    virtual SavexmlResponse* Savexml(EsdlContext* context, SavexmlRequest* request);
    virtual SuperfileActionResponse* SuperfileAction(EsdlContext* context, SuperfileActionRequest* request);
    virtual SuperfileListResponse* SuperfileList(EsdlContext* context, SuperfileListRequest* request);
    virtual int Add(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::Add(CtxStr, ReqStr, RespStr);
    }
    virtual int AddRemote(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::AddRemote(CtxStr, ReqStr, RespStr);
    }
    virtual int AddtoSuperfile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::AddtoSuperfile(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUArrayAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUArrayAction(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUBrowseData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUBrowseData(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUDefFile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUDefFile(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFileAccess(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFileAccess(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFileAccessV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFileAccessV2(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFileCreate(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFileCreate(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFileCreateV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFileCreateV2(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFilePublish(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFilePublish(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUFileView(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUFileView(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUGetDataColumns(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUGetDataColumns(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUGetFileMetaData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUGetFileMetaData(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUInfo(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUQuery(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUQuery(CtxStr, ReqStr, RespStr);
    }
    virtual int DFURecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFURecordTypeInfo(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUSearch(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUSearch(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUSearchData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUSearchData(CtxStr, ReqStr, RespStr);
    }
    virtual int DFUSpace(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::DFUSpace(CtxStr, ReqStr, RespStr);
    }
    virtual int EclRecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::EclRecordTypeInfo(CtxStr, ReqStr, RespStr);
    }
    virtual int EraseHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::EraseHistory(CtxStr, ReqStr, RespStr);
    }
    virtual int ListHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::ListHistory(CtxStr, ReqStr, RespStr);
    }
    virtual int Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::Ping(CtxStr, ReqStr, RespStr);
    }
    virtual int Savexml(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::Savexml(CtxStr, ReqStr, RespStr);
    }
    virtual int SuperfileAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::SuperfileAction(CtxStr, ReqStr, RespStr);
    }
    virtual int SuperfileList(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsDfuServiceBase::SuperfileList(CtxStr, ReqStr, RespStr);
    }
};

#endif
