#ifndef WSTESTSERVICEBASE_HPP__
#define WSTESTSERVICEBASE_HPP__

#include "jlib.hpp"
#include "jptree.hpp"
#include "jarray.hpp"
#include "primitivetypes.hpp"

using namespace std;
using namespace cppplugin;

class EsdlContext : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> username;
    Owned<Integer> clientMajorVersion;
    Owned<Integer> clientMinorVersion;
};


enum class AnnotatedEnum
{
    UNSET = 0,
    Fus,
    Ro,
    Dah
};


class EnumHandlerAnnotatedEnum
{
public:
    static AnnotatedEnum fromString(const char* str)
    {
        if (strcmp(str, "1") == 0)
            return AnnotatedEnum::Fus;
        else if (strcmp(str, "2") == 0)
            return AnnotatedEnum::Ro;
        else
            return AnnotatedEnum::Dah;

    }

    static const char* toString(AnnotatedEnum val)
    {
        if (val == AnnotatedEnum::Fus)
            return "1";
        else if (val == AnnotatedEnum::Ro)
            return "2";
        else
            return "3";

    }
};

class FooBar : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_Foo;
    Owned<PString> m_Bar;
};

class AllVersionArrays : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    IArrayOf<PString> m_StringArray;
    IArrayOf<FooBar> m_FooBarArray;
    IArrayOf<FooBar> m_NamedItemFooBarArray;
};

class AllVersionReportRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_OptionalDeveloperStringVal;
    Owned<Integer> m_Annotate20ColsIntVal;
    AnnotatedEnum m_UnrelentingForce = EnumHandlerAnnotatedEnum::fromString("1");
    Owned<AllVersionArrays> m_Arrays;
};

class MinVersionReportRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_RequestString;
};

class WsTestPingRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class VersionRangeReportRequest : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_RequestString;
};

class VersionRangeReportResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ResponseString;
};

class AllVersionReportResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ResultVal;
    Owned<AllVersionArrays> m_ResultArrays;
};

class MinVersionReportResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    Owned<PString> m_ResponseString;
};

class WsTestPingResponse : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

};

class WsTestServiceBase : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual AllVersionReportResponse* AllVersionReport(EsdlContext* context, AllVersionReportRequest* request){return nullptr;}
    virtual MinVersionReportResponse* MinVersion(EsdlContext* context, MinVersionReportRequest* request){return nullptr;}
    virtual WsTestPingResponse* Ping(EsdlContext* context, WsTestPingRequest* request){return nullptr;}
    virtual VersionRangeReportResponse* VersionRangeReport(EsdlContext* context, VersionRangeReportRequest* request){return nullptr;}
    virtual int AllVersionReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int MinVersion(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
    virtual int VersionRangeReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
};

// Implemented in generated code
extern "C" int onWsTestAllVersionReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsTestMinVersion(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsTestPing(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);
extern "C" int onWsTestVersionRangeReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr);

// User need to implement this function
extern "C" WsTestServiceBase* createWsTestServiceObj();


#endif