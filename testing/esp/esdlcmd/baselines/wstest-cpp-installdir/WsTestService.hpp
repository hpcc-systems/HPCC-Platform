#ifndef WSTESTSERVICE_HPP__
#define WSTESTSERVICE_HPP__

#include "jlib.hpp"
#include "WsTestServiceBase.hpp"

using namespace std;


class WsTestService : public WsTestServiceBase
{
public:
        virtual AllVersionReportResponse* AllVersionReport(EsdlContext* context, AllVersionReportRequest* request);
    virtual MinVersionReportResponse* MinVersion(EsdlContext* context, MinVersionReportRequest* request);
    virtual WsTestPingResponse* Ping(EsdlContext* context, WsTestPingRequest* request);
    virtual VersionRangeReportResponse* VersionRangeReport(EsdlContext* context, VersionRangeReportRequest* request);
    virtual int AllVersionReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsTestServiceBase::AllVersionReport(CtxStr, ReqStr, RespStr);
    }
    virtual int MinVersion(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsTestServiceBase::MinVersion(CtxStr, ReqStr, RespStr);
    }
    virtual int Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsTestServiceBase::Ping(CtxStr, ReqStr, RespStr);
    }
    virtual int VersionRangeReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
    {
        return WsTestServiceBase::VersionRangeReport(CtxStr, ReqStr, RespStr);
    }
};

#endif
