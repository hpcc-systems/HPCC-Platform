#include "EsdlExampleService.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

CppEchoPersonInfoResponse* EsdlExampleService::CppEchoPersonInfo(EsdlContext* context, CppEchoPersonInfoRequest* request)
{
    Owned<CppEchoPersonInfoResponse> resp = new CppEchoPersonInfoResponse();
    //Fill in logic
    resp->m_count.setown(new Integer(0));
    if (request->m_Name)
    {
        resp->m_count.setown(new Integer(1));
        resp->m_Name.set(request->m_Name.get());
    }
    appendArray(resp->m_Addresses, request->m_Addresses);
    return resp.getClear();
}

JavaEchoPersonInfoResponse* EsdlExampleService::JavaEchoPersonInfo(EsdlContext* context, JavaEchoPersonInfoRequest* request)
{
    Owned<JavaEchoPersonInfoResponse> resp = new JavaEchoPersonInfoResponse();
    //Fill in logic
    return resp.getClear();
}

EsdlExamplePingResponse* EsdlExampleService::Ping(EsdlContext* context, EsdlExamplePingRequest* request)
{
    Owned<EsdlExamplePingResponse> resp = new EsdlExamplePingResponse();
    //Fill in logic
    return resp.getClear();
}

RoxieEchoPersonInfoResponse* EsdlExampleService::RoxieEchoPersonInfo(EsdlContext* context, RoxieEchoPersonInfoRequest* request)
{
    Owned<RoxieEchoPersonInfoResponse> resp = new RoxieEchoPersonInfoResponse();
    //Fill in logic
    return resp.getClear();
}

extern "C" EsdlExampleServiceBase* createEsdlExampleServiceObj()
{
    return new EsdlExampleService();
}
