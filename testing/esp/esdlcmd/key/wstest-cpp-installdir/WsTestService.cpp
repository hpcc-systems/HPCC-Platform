#include "WsTestService.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

AllVersionReportResponse* WsTestService::AllVersionReport(EsdlContext* context, AllVersionReportRequest* request)
{
    Owned<AllVersionReportResponse> resp = new AllVersionReportResponse();
    //Fill in logic
    return resp.getClear();
}

MinVersionReportResponse* WsTestService::MinVersion(EsdlContext* context, MinVersionReportRequest* request)
{
    Owned<MinVersionReportResponse> resp = new MinVersionReportResponse();
    //Fill in logic
    return resp.getClear();
}

WsTestPingResponse* WsTestService::Ping(EsdlContext* context, WsTestPingRequest* request)
{
    Owned<WsTestPingResponse> resp = new WsTestPingResponse();
    //Fill in logic
    return resp.getClear();
}

VersionRangeReportResponse* WsTestService::VersionRangeReport(EsdlContext* context, VersionRangeReportRequest* request)
{
    Owned<VersionRangeReportResponse> resp = new VersionRangeReportResponse();
    //Fill in logic
    return resp.getClear();
}

extern "C" WsTestServiceBase* createWsTestServiceObj()
{
    return new WsTestService();
}
