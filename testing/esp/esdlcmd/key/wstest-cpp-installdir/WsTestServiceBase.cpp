
#include "WsTestServiceBase.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

extern "C" int onWsTestAllVersionReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    return service->AllVersionReport(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsTestMinVersion(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    return service->MinVersion(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsTestPing(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    return service->Ping(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsTestVersionRangeReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    return service->VersionRangeReport(CtxStr, ReqStr, RespStr);
}


class WsTestUnSerializer : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    WsTestUnSerializer()
    {
    }

    int unserialize(EsdlContext* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        if (ptree->hasProp("username"))
            obj->username.setown(new PString(ptree->queryProp("username")));
        if (ptree->hasProp("clientMajorVersion"))
            obj->clientMajorVersion.setown(new Integer(ptree->getPropInt("clientMajorVersion")));
        if (ptree->hasProp("clientMinorVersion"))
            obj->clientMinorVersion.setown(new Integer(ptree->getPropInt("clientMinorVersion")));
    }

    int unserialize(FooBar* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Foo"))
            obj->m_Foo.setown(new PString(ptree->queryProp("Foo")));

        if (ptree->hasProp("Bar"))
            obj->m_Bar.setown(new PString(ptree->queryProp("Bar")));

        return 0;
    }

    int unserialize(AllVersionArrays* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        subtree = ptree->queryPropTree("StringArray");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_StringArray.append(*(new PString(onetree->queryProp("."))));

            }

         }

        subtree = ptree->queryPropTree("FooBarArray");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<FooBar> oneobj(new FooBar());
                unserialize(oneobj.get(), onetree);
                obj->m_FooBarArray.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("NamedItemFooBarArray");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("NamedItem");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<FooBar> oneobj(new FooBar());
                unserialize(oneobj.get(), onetree);
                obj->m_NamedItemFooBarArray.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(AllVersionReportRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("OptionalDeveloperStringVal"))
            obj->m_OptionalDeveloperStringVal.setown(new PString(ptree->queryProp("OptionalDeveloperStringVal")));

        if (ptree->hasProp("Annotate20ColsIntVal"))
            obj->m_Annotate20ColsIntVal.setown(new Integer(ptree->getPropInt("Annotate20ColsIntVal")));

        if (ptree->hasProp("UnrelentingForce"))
            obj->m_UnrelentingForce = EnumHandlerAnnotatedEnum::fromString(ptree->queryProp("UnrelentingForce"));

        subtree = ptree->queryPropTree("Arrays");
        if (subtree != nullptr)
        {
            obj->m_Arrays.setown(new AllVersionArrays());
            unserialize(obj->m_Arrays.get(), subtree);
        }

        return 0;
    }

    int unserialize(AllVersionReportResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ResultVal"))
            obj->m_ResultVal.setown(new PString(ptree->queryProp("ResultVal")));

        subtree = ptree->queryPropTree("ResultArrays");
        if (subtree != nullptr)
        {
            obj->m_ResultArrays.setown(new AllVersionArrays());
            unserialize(obj->m_ResultArrays.get(), subtree);
        }

        return 0;
    }

    int unserialize(MinVersionReportRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("RequestString"))
            obj->m_RequestString.setown(new PString(ptree->queryProp("RequestString")));

        return 0;
    }

    int unserialize(MinVersionReportResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ResponseString"))
            obj->m_ResponseString.setown(new PString(ptree->queryProp("ResponseString")));

        return 0;
    }

    int unserialize(WsTestPingRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(WsTestPingResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(VersionRangeReportRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("RequestString"))
            obj->m_RequestString.setown(new PString(ptree->queryProp("RequestString")));

        return 0;
    }

    int unserialize(VersionRangeReportResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ResponseString"))
            obj->m_ResponseString.setown(new PString(ptree->queryProp("ResponseString")));

        return 0;
    }

    int serialize(FooBar* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Foo)
        {
            buf.append("<Foo>");
            buf.append(obj->m_Foo->str());
            buf.append("</Foo>");
        }

        if (obj->m_Bar)
        {
            buf.append("<Bar>");
            buf.append(obj->m_Bar->str());
            buf.append("</Bar>");
        }

        return 0;
    }

    int serialize(AllVersionArrays* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_StringArray.length() > 0)
        {
            buf.append("<StringArray>");
            ForEachItemIn(i, obj->m_StringArray)
            {
            PString& oneitem = obj->m_StringArray.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</StringArray>");
        }

        if (obj->m_FooBarArray.length() > 0)
        {
            buf.append("<FooBarArray>");
            ForEachItemIn(i, obj->m_FooBarArray)
            {
                    FooBar& oneitem = obj->m_FooBarArray.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</FooBarArray>");
        }

        if (obj->m_NamedItemFooBarArray.length() > 0)
        {
            buf.append("<NamedItemFooBarArray>");
            ForEachItemIn(i, obj->m_NamedItemFooBarArray)
            {
                    FooBar& oneitem = obj->m_NamedItemFooBarArray.item(i);
                buf.append("<NamedItem>");
                serialize(&oneitem, buf);
                buf.append("</NamedItem>");

            }
            buf.append("</NamedItemFooBarArray>");
        }

        return 0;
    }

    int serialize(AllVersionReportRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_OptionalDeveloperStringVal)
        {
            buf.append("<OptionalDeveloperStringVal>");
            buf.append(obj->m_OptionalDeveloperStringVal->str());
            buf.append("</OptionalDeveloperStringVal>");
        }

        if (obj->m_Annotate20ColsIntVal)
        {
            buf.append("<Annotate20ColsIntVal>");
            buf.append(obj->m_Annotate20ColsIntVal->str());
            buf.append("</Annotate20ColsIntVal>");
        }

        if (obj->m_UnrelentingForce != AnnotatedEnum::UNSET)
            buf.append("<UnrelentingForce>").append(EnumHandlerAnnotatedEnum::toString(obj->m_UnrelentingForce)).append("</UnrelentingForce>");

        if (obj->m_Arrays)
        {
            buf.append("<Arrays>");
            serialize(obj->m_Arrays, buf);
            buf.append("</Arrays>");
        }

        return 0;
    }

    int serialize(AllVersionReportResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ResultVal)
        {
            buf.append("<ResultVal>");
            buf.append(obj->m_ResultVal->str());
            buf.append("</ResultVal>");
        }

        if (obj->m_ResultArrays)
        {
            buf.append("<ResultArrays>");
            serialize(obj->m_ResultArrays, buf);
            buf.append("</ResultArrays>");
        }

        return 0;
    }

    int serialize(MinVersionReportRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_RequestString)
        {
            buf.append("<RequestString>");
            buf.append(obj->m_RequestString->str());
            buf.append("</RequestString>");
        }

        return 0;
    }

    int serialize(MinVersionReportResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ResponseString)
        {
            buf.append("<ResponseString>");
            buf.append(obj->m_ResponseString->str());
            buf.append("</ResponseString>");
        }

        return 0;
    }

    int serialize(WsTestPingRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(WsTestPingResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(VersionRangeReportRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_RequestString)
        {
            buf.append("<RequestString>");
            buf.append(obj->m_RequestString->str());
            buf.append("</RequestString>");
        }

        return 0;
    }

    int serialize(VersionRangeReportResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ResponseString)
        {
            buf.append("<ResponseString>");
            buf.append(obj->m_ResponseString->str());
            buf.append("</ResponseString>");
        }

        return 0;
    }

};
int WsTestServiceBase::AllVersionReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestUnSerializer> UnSe = new WsTestUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    Owned<AllVersionReportRequest> req = new AllVersionReportRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<AllVersionReportResponse> resp = service->AllVersionReport(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"AllVersionReportResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsTestServiceBase::MinVersion(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestUnSerializer> UnSe = new WsTestUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    Owned<MinVersionReportRequest> req = new MinVersionReportRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<MinVersionReportResponse> resp = service->MinVersion(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"MinVersionReportResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsTestServiceBase::Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestUnSerializer> UnSe = new WsTestUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    Owned<WsTestPingRequest> req = new WsTestPingRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<WsTestPingResponse> resp = service->Ping(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"WsTestPingResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsTestServiceBase::VersionRangeReport(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsTestUnSerializer> UnSe = new WsTestUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsTestServiceBase> service = createWsTestServiceObj();
    Owned<VersionRangeReportRequest> req = new VersionRangeReportRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<VersionRangeReportResponse> resp = service->VersionRangeReport(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"VersionRangeReportResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
