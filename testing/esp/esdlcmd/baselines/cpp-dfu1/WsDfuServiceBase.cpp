
#include "WsDfuServiceBase.hpp"
#include "jliball.hpp"
#include "jlog.hpp"
#include "jptree.hpp"

extern "C" int onWsDfuAdd(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->Add(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuAddRemote(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->AddRemote(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuAddtoSuperfile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->AddtoSuperfile(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUArrayAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUArrayAction(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUBrowseData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUBrowseData(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUDefFile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUDefFile(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFileAccess(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFileAccess(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFileAccessV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFileAccessV2(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFileCreate(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFileCreate(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFileCreateV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFileCreateV2(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFilePublish(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFilePublish(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUFileView(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUFileView(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUGetDataColumns(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUGetDataColumns(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUGetFileMetaData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUGetFileMetaData(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUInfo(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUQuery(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUQuery(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFURecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFURecordTypeInfo(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUSearch(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUSearch(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUSearchData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUSearchData(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuDFUSpace(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->DFUSpace(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuEclRecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->EclRecordTypeInfo(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuEraseHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->EraseHistory(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuListHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->ListHistory(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuPing(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->Ping(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuSavexml(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->Savexml(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuSuperfileAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->SuperfileAction(CtxStr, ReqStr, RespStr);
}

extern "C" int onWsDfuSuperfileList(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    return service->SuperfileList(CtxStr, ReqStr, RespStr);
}


class WsDfuUnSerializer : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    WsDfuUnSerializer()
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

    int unserialize(AddRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("dstname"))
            obj->m_dstname.setown(new PString(ptree->queryProp("dstname")));

        if (ptree->hasProp("xmlmap"))
            obj->m_xmlmap.setown(new PString(ptree->queryProp("xmlmap")));

        return 0;
    }

    int unserialize(AddResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(AddRemoteRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("dstname"))
            obj->m_dstname.setown(new PString(ptree->queryProp("dstname")));

        if (ptree->hasProp("srcname"))
            obj->m_srcname.setown(new PString(ptree->queryProp("srcname")));

        if (ptree->hasProp("srcdali"))
            obj->m_srcdali.setown(new PString(ptree->queryProp("srcdali")));

        if (ptree->hasProp("srcusername"))
            obj->m_srcusername.setown(new PString(ptree->queryProp("srcusername")));

        if (ptree->hasProp("srcpassword"))
            obj->m_srcpassword.setown(new PString(ptree->queryProp("srcpassword")));

        return 0;
    }

    int unserialize(AddRemoteResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(AddtoSuperfileRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Superfile"))
            obj->m_Superfile.setown(new PString(ptree->queryProp("Superfile")));

        if (ptree->hasProp("Subfiles"))
            obj->m_Subfiles.setown(new PString(ptree->queryProp("Subfiles")));

        subtree = ptree->queryPropTree("names");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_names.append(*(new PString(onetree->queryProp("."))));

            }

         }

        if (ptree->hasProp("ExistingFile"))
            obj->m_ExistingFile.setown(new Boolean(ptree->getPropBool("ExistingFile")));

        if (ptree->hasProp("BackToPage"))
            obj->m_BackToPage.setown(new PString(ptree->queryProp("BackToPage")));

        return 0;
    }

    int unserialize(AddtoSuperfileResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Subfiles"))
            obj->m_Subfiles.setown(new PString(ptree->queryProp("Subfiles")));

        if (ptree->hasProp("BackToPage"))
            obj->m_BackToPage.setown(new PString(ptree->queryProp("BackToPage")));

        subtree = ptree->queryPropTree("SubfileNames");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("SubfileName");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_SubfileNames.append(*(new PString(onetree->queryProp("."))));

            }

         }

        return 0;
    }

    int unserialize(DFUArrayActionRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Type"))
            obj->m_Type = EnumHandlerDFUArrayActions::fromString(ptree->queryProp("Type"));

        if (ptree->hasProp("NoDelete"))
            obj->m_NoDelete.setown(new Boolean(ptree->getPropBool("NoDelete")));

        if (ptree->hasProp("BackToPage"))
            obj->m_BackToPage.setown(new PString(ptree->queryProp("BackToPage")));

        subtree = ptree->queryPropTree("LogicalFiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_LogicalFiles.append(*(new PString(onetree->queryProp("."))));

            }

         }

        if (ptree->hasProp("removeFromSuperfiles"))
            obj->m_removeFromSuperfiles.setown(new Boolean(ptree->getPropBool("removeFromSuperfiles")));

        if (ptree->hasProp("removeRecursively"))
            obj->m_removeRecursively.setown(new Boolean(ptree->getPropBool("removeRecursively")));

        if (ptree->hasProp("Protect"))
            obj->m_Protect = EnumHandlerDFUChangeProtection::fromString(ptree->queryProp("Protect"));

        if (ptree->hasProp("Restrict"))
            obj->m_Restrict = EnumHandlerDFUChangeRestriction::fromString(ptree->queryProp("Restrict"));

        return 0;
    }

    int unserialize(DFUActionInfo* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("FileName"))
            obj->m_FileName.setown(new PString(ptree->queryProp("FileName")));

        if (ptree->hasProp("NodeGroup"))
            obj->m_NodeGroup.setown(new PString(ptree->queryProp("NodeGroup")));

        if (ptree->hasProp("ActionResult"))
            obj->m_ActionResult.setown(new PString(ptree->queryProp("ActionResult")));

        if (ptree->hasProp("Failed"))
            obj->m_Failed.setown(new Boolean(ptree->getPropBool("Failed")));

        return 0;
    }

    int unserialize(DFUArrayActionResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("BackToPage"))
            obj->m_BackToPage.setown(new PString(ptree->queryProp("BackToPage")));

        if (ptree->hasProp("RedirectTo"))
            obj->m_RedirectTo.setown(new PString(ptree->queryProp("RedirectTo")));

        if (ptree->hasProp("DFUArrayActionResult"))
            obj->m_DFUArrayActionResult.setown(new PString(ptree->queryProp("DFUArrayActionResult")));

        subtree = ptree->queryPropTree("ActionResults");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUActionInfo> oneobj(new DFUActionInfo());
                unserialize(oneobj.get(), onetree);
                obj->m_ActionResults.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(DFUBrowseDataRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("FilterBy"))
            obj->m_FilterBy.setown(new PString(ptree->queryProp("FilterBy")));

        if (ptree->hasProp("ShowColumns"))
            obj->m_ShowColumns.setown(new PString(ptree->queryProp("ShowColumns")));

        if (ptree->hasProp("SchemaOnly"))
            obj->m_SchemaOnly.setown(new Boolean(ptree->getPropBool("SchemaOnly")));

        if (ptree->hasProp("StartForGoback"))
            obj->m_StartForGoback.setown(new Integer64(ptree->getPropInt64("StartForGoback")));

        if (ptree->hasProp("CountForGoback"))
            obj->m_CountForGoback.setown(new Integer(ptree->getPropInt("CountForGoback")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        if (ptree->hasProp("ParentName"))
            obj->m_ParentName.setown(new PString(ptree->queryProp("ParentName")));

        if (ptree->hasProp("Start"))
            obj->m_Start.setown(new Integer64(ptree->getPropInt64("Start")));

        if (ptree->hasProp("Count"))
            obj->m_Count.setown(new Integer(ptree->getPropInt("Count")));

        if (ptree->hasProp("DisableUppercaseTranslation"))
            obj->m_DisableUppercaseTranslation.setown(new Boolean(ptree->getPropBool("DisableUppercaseTranslation")));

        return 0;
    }

    int unserialize(DFUDataColumn* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ColumnID"))
            obj->m_ColumnID.setown(new Integer(ptree->getPropInt("ColumnID")));

        if (ptree->hasProp("ColumnLabel"))
            obj->m_ColumnLabel.setown(new PString(ptree->queryProp("ColumnLabel")));

        if (ptree->hasProp("ColumnType"))
            obj->m_ColumnType.setown(new PString(ptree->queryProp("ColumnType")));

        if (ptree->hasProp("ColumnValue"))
            obj->m_ColumnValue.setown(new PString(ptree->queryProp("ColumnValue")));

        if (ptree->hasProp("ColumnSize"))
            obj->m_ColumnSize.setown(new Integer(ptree->getPropInt("ColumnSize")));

        if (ptree->hasProp("MaxSize"))
            obj->m_MaxSize.setown(new Integer(ptree->getPropInt("MaxSize")));

        if (ptree->hasProp("ColumnEclType"))
            obj->m_ColumnEclType.setown(new PString(ptree->queryProp("ColumnEclType")));

        if (ptree->hasProp("ColumnRawSize"))
            obj->m_ColumnRawSize.setown(new Integer(ptree->getPropInt("ColumnRawSize")));

        if (ptree->hasProp("IsNaturalColumn"))
            obj->m_IsNaturalColumn.setown(new Boolean(ptree->getPropBool("IsNaturalColumn")));

        if (ptree->hasProp("IsKeyedColumn"))
            obj->m_IsKeyedColumn.setown(new Boolean(ptree->getPropBool("IsKeyedColumn")));

        subtree = ptree->queryPropTree("DataColumns");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DataColumns.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(DFUBrowseDataResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("FilterBy"))
            obj->m_FilterBy.setown(new PString(ptree->queryProp("FilterBy")));

        if (ptree->hasProp("FilterForGoBack"))
            obj->m_FilterForGoBack.setown(new PString(ptree->queryProp("FilterForGoBack")));

        subtree = ptree->queryPropTree("ColumnsHidden");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("ColumnHidden");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_ColumnsHidden.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("ColumnCount"))
            obj->m_ColumnCount.setown(new Integer(ptree->getPropInt("ColumnCount")));

        if (ptree->hasProp("StartForGoback"))
            obj->m_StartForGoback.setown(new Integer64(ptree->getPropInt64("StartForGoback")));

        if (ptree->hasProp("CountForGoback"))
            obj->m_CountForGoback.setown(new Integer(ptree->getPropInt("CountForGoback")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("SchemaOnly"))
            obj->m_SchemaOnly.setown(new Boolean(ptree->getPropBool("SchemaOnly")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        if (ptree->hasProp("ParentName"))
            obj->m_ParentName.setown(new PString(ptree->queryProp("ParentName")));

        if (ptree->hasProp("Start"))
            obj->m_Start.setown(new Integer64(ptree->getPropInt64("Start")));

        if (ptree->hasProp("Count"))
            obj->m_Count.setown(new Integer64(ptree->getPropInt64("Count")));

        if (ptree->hasProp("PageSize"))
            obj->m_PageSize.setown(new Integer64(ptree->getPropInt64("PageSize")));

        if (ptree->hasProp("Total"))
            obj->m_Total.setown(new Integer64(ptree->getPropInt64("Total")));

        if (ptree->hasProp("Result"))
            obj->m_Result.setown(new PString(ptree->queryProp("Result")));

        if (ptree->hasProp("MsgToDisplay"))
            obj->m_MsgToDisplay.setown(new PString(ptree->queryProp("MsgToDisplay")));

        if (ptree->hasProp("DisableUppercaseTranslation"))
            obj->m_DisableUppercaseTranslation.setown(new Boolean(ptree->getPropBool("DisableUppercaseTranslation")));

        return 0;
    }

    int unserialize(DFUDefFileRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Format"))
            obj->m_Format = EnumHandlerDFUDefFileFormat::fromString(ptree->queryProp("Format"));

        return 0;
    }

    int unserialize(DFUDefFileResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("defFile"))
            obj->m_defFile.setown(new PString(ptree->queryProp("defFile")));

        return 0;
    }

    int unserialize(DFUFileAccessRequestBase* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("JobId"))
            obj->m_JobId.setown(new PString(ptree->queryProp("JobId")));

        if (ptree->hasProp("ExpirySeconds"))
            obj->m_ExpirySeconds.setown(new Integer(ptree->getPropInt("ExpirySeconds")));

        if (ptree->hasProp("AccessRole"))
            obj->m_AccessRole = EnumHandlerFileAccessRole::fromString(ptree->queryProp("AccessRole"));

        if (ptree->hasProp("AccessType"))
            obj->m_AccessType = EnumHandlerSecAccessType::fromString(ptree->queryProp("AccessType"));

        if (ptree->hasProp("ReturnJsonTypeInfo"))
            obj->m_ReturnJsonTypeInfo.setown(new Boolean(ptree->getPropBool("ReturnJsonTypeInfo")));

        if (ptree->hasProp("ReturnBinTypeInfo"))
            obj->m_ReturnBinTypeInfo.setown(new Boolean(ptree->getPropBool("ReturnBinTypeInfo")));

        return 0;
    }

    int unserialize(DFUFileAccessRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        subtree = ptree->queryPropTree("RequestBase");
        if (subtree != nullptr)
        {
            obj->m_RequestBase.setown(new DFUFileAccessRequestBase());
            unserialize(obj->m_RequestBase.get(), subtree);
        }

        return 0;
    }

    int unserialize(DFUPartLocation* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("LocationIndex"))
            obj->m_LocationIndex.setown(new Integer(ptree->getPropInt("LocationIndex")));

        if (ptree->hasProp("Host"))
            obj->m_Host.setown(new PString(ptree->queryProp("Host")));

        return 0;
    }

    int unserialize(DFUFileCopy* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("CopyIndex"))
            obj->m_CopyIndex.setown(new Integer(ptree->getPropInt("CopyIndex")));

        if (ptree->hasProp("LocationIndex"))
            obj->m_LocationIndex.setown(new Integer(ptree->getPropInt("LocationIndex")));

        if (ptree->hasProp("Path"))
            obj->m_Path.setown(new PString(ptree->queryProp("Path")));

        return 0;
    }

    int unserialize(DFUFilePart* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("PartIndex"))
            obj->m_PartIndex.setown(new Integer(ptree->getPropInt("PartIndex")));

        subtree = ptree->queryPropTree("Copies");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUFileCopy> oneobj(new DFUFileCopy());
                unserialize(oneobj.get(), onetree);
                obj->m_Copies.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("TopLevelKey"))
            obj->m_TopLevelKey.setown(new Boolean(ptree->getPropBool("TopLevelKey")));

        return 0;
    }

    int unserialize(DFUFileAccessInfo* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("MetaInfoBlob"))
            obj->m_MetaInfoBlob.setown(new PString(ptree->queryProp("MetaInfoBlob")));

        if (ptree->hasProp("ExpiryTime"))
            obj->m_ExpiryTime.setown(new PString(ptree->queryProp("ExpiryTime")));

        if (ptree->hasProp("NumParts"))
            obj->m_NumParts.setown(new Integer(ptree->getPropInt("NumParts")));

        subtree = ptree->queryPropTree("FileLocations");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUPartLocation> oneobj(new DFUPartLocation());
                unserialize(oneobj.get(), onetree);
                obj->m_FileLocations.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("FileParts");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUFilePart> oneobj(new DFUFilePart());
                unserialize(oneobj.get(), onetree);
                obj->m_FileParts.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("RecordTypeInfoBin"))
            obj->m_RecordTypeInfoBin.setown(new PString(ptree->queryProp("RecordTypeInfoBin")));

        if (ptree->hasProp("RecordTypeInfoJson"))
            obj->m_RecordTypeInfoJson.setown(new PString(ptree->queryProp("RecordTypeInfoJson")));

        if (ptree->hasProp("fileAccessPort"))
            obj->m_fileAccessPort.setown(new Integer(ptree->getPropInt("fileAccessPort")));

        if (ptree->hasProp("fileAccessSSL"))
            obj->m_fileAccessSSL.setown(new Boolean(ptree->getPropBool("fileAccessSSL")));

        return 0;
    }

    int unserialize(DFUFileAccessResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        subtree = ptree->queryPropTree("AccessInfo");
        if (subtree != nullptr)
        {
            obj->m_AccessInfo.setown(new DFUFileAccessInfo());
            unserialize(obj->m_AccessInfo.get(), subtree);
        }

        if (ptree->hasProp("Type"))
            obj->m_Type = EnumHandlerDFUFileType::fromString(ptree->queryProp("Type"));

        return 0;
    }

    int unserialize(DFUFileAccessV2Request* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("RequestId"))
            obj->m_RequestId.setown(new PString(ptree->queryProp("RequestId")));

        if (ptree->hasProp("ExpirySeconds"))
            obj->m_ExpirySeconds.setown(new Integer(ptree->getPropInt("ExpirySeconds")));

        if (ptree->hasProp("ReturnTextResponse"))
            obj->m_ReturnTextResponse.setown(new Boolean(ptree->getPropBool("ReturnTextResponse")));

        if (ptree->hasProp("SessionId"))
            obj->m_SessionId.setown(new Integer64(ptree->getPropInt64("SessionId")));

        if (ptree->hasProp("LockTimeoutMs"))
            obj->m_LockTimeoutMs.setown(new Integer(ptree->getPropInt("LockTimeoutMs")));

        return 0;
    }

    int unserialize(DFUFileCreateRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ECLRecordDefinition"))
            obj->m_ECLRecordDefinition.setown(new PString(ptree->queryProp("ECLRecordDefinition")));

        subtree = ptree->queryPropTree("PartLocations");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_PartLocations.append(*(new PString(onetree->queryProp("."))));

            }

         }

        subtree = ptree->queryPropTree("RequestBase");
        if (subtree != nullptr)
        {
            obj->m_RequestBase.setown(new DFUFileAccessRequestBase());
            unserialize(obj->m_RequestBase.get(), subtree);
        }

        return 0;
    }

    int unserialize(DFUFileCreateResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("FileId"))
            obj->m_FileId.setown(new PString(ptree->queryProp("FileId")));

        if (ptree->hasProp("Warning"))
            obj->m_Warning.setown(new PString(ptree->queryProp("Warning")));

        subtree = ptree->queryPropTree("AccessInfo");
        if (subtree != nullptr)
        {
            obj->m_AccessInfo.setown(new DFUFileAccessInfo());
            unserialize(obj->m_AccessInfo.get(), subtree);
        }

        return 0;
    }

    int unserialize(DFUFileCreateV2Request* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("Type"))
            obj->m_Type = EnumHandlerDFUFileType::fromString(ptree->queryProp("Type"));

        if (ptree->hasProp("ECLRecordDefinition"))
            obj->m_ECLRecordDefinition.setown(new PString(ptree->queryProp("ECLRecordDefinition")));

        if (ptree->hasProp("RequestId"))
            obj->m_RequestId.setown(new PString(ptree->queryProp("RequestId")));

        if (ptree->hasProp("ExpirySeconds"))
            obj->m_ExpirySeconds.setown(new Integer(ptree->getPropInt("ExpirySeconds")));

        if (ptree->hasProp("ReturnTextResponse"))
            obj->m_ReturnTextResponse.setown(new Boolean(ptree->getPropBool("ReturnTextResponse")));

        if (ptree->hasProp("Compressed"))
            obj->m_Compressed.setown(new Boolean(ptree->getPropBool("Compressed")));

        if (ptree->hasProp("SessionId"))
            obj->m_SessionId.setown(new Integer64(ptree->getPropInt64("SessionId")));

        if (ptree->hasProp("LockTimeoutMs"))
            obj->m_LockTimeoutMs.setown(new Integer(ptree->getPropInt("LockTimeoutMs")));

        return 0;
    }

    int unserialize(DFUFilePublishRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("FileId"))
            obj->m_FileId.setown(new PString(ptree->queryProp("FileId")));

        if (ptree->hasProp("Overwrite"))
            obj->m_Overwrite.setown(new Boolean(ptree->getPropBool("Overwrite")));

        if (ptree->hasProp("FileDescriptorBlob"))
            obj->m_FileDescriptorBlob.setown(new PString(ptree->queryProp("FileDescriptorBlob")));

        if (ptree->hasProp("SessionId"))
            obj->m_SessionId.setown(new Integer64(ptree->getPropInt64("SessionId")));

        if (ptree->hasProp("LockTimeoutMs"))
            obj->m_LockTimeoutMs.setown(new Integer(ptree->getPropInt("LockTimeoutMs")));

        if (ptree->hasProp("ECLRecordDefinition"))
            obj->m_ECLRecordDefinition.setown(new PString(ptree->queryProp("ECLRecordDefinition")));

        if (ptree->hasProp("RecordCount"))
            obj->m_RecordCount.setown(new Integer64(ptree->getPropInt64("RecordCount")));

        if (ptree->hasProp("FileSize"))
            obj->m_FileSize.setown(new Integer64(ptree->getPropInt64("FileSize")));

        return 0;
    }

    int unserialize(DFUFilePublishResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(DFUFileViewRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Scope"))
            obj->m_Scope.setown(new PString(ptree->queryProp("Scope")));

        if (ptree->hasProp("IncludeSuperOwner"))
            obj->m_IncludeSuperOwner.setown(new Boolean(ptree->getPropBool("IncludeSuperOwner")));

        return 0;
    }

    int unserialize(DFULogicalFile* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Prefix"))
            obj->m_Prefix.setown(new PString(ptree->queryProp("Prefix")));

        if (ptree->hasProp("ClusterName"))
            obj->m_ClusterName.setown(new PString(ptree->queryProp("ClusterName")));

        if (ptree->hasProp("NodeGroup"))
            obj->m_NodeGroup.setown(new PString(ptree->queryProp("NodeGroup")));

        if (ptree->hasProp("Directory"))
            obj->m_Directory.setown(new PString(ptree->queryProp("Directory")));

        if (ptree->hasProp("Description"))
            obj->m_Description.setown(new PString(ptree->queryProp("Description")));

        if (ptree->hasProp("Parts"))
            obj->m_Parts.setown(new PString(ptree->queryProp("Parts")));

        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("Totalsize"))
            obj->m_Totalsize.setown(new PString(ptree->queryProp("Totalsize")));

        if (ptree->hasProp("RecordCount"))
            obj->m_RecordCount.setown(new PString(ptree->queryProp("RecordCount")));

        if (ptree->hasProp("Modified"))
            obj->m_Modified.setown(new PString(ptree->queryProp("Modified")));

        if (ptree->hasProp("LongSize"))
            obj->m_LongSize.setown(new PString(ptree->queryProp("LongSize")));

        if (ptree->hasProp("LongRecordCount"))
            obj->m_LongRecordCount.setown(new PString(ptree->queryProp("LongRecordCount")));

        if (ptree->hasProp("isSuperfile"))
            obj->m_isSuperfile.setown(new Boolean(ptree->getPropBool("isSuperfile")));

        if (ptree->hasProp("isZipfile"))
            obj->m_isZipfile.setown(new Boolean(ptree->getPropBool("isZipfile")));

        if (ptree->hasProp("isDirectory"))
            obj->m_isDirectory.setown(new Boolean(ptree->getPropBool("isDirectory")));

        if (ptree->hasProp("Replicate"))
            obj->m_Replicate.setown(new Boolean(ptree->getPropBool("Replicate")));

        if (ptree->hasProp("IntSize"))
            obj->m_IntSize.setown(new Integer64(ptree->getPropInt64("IntSize")));

        if (ptree->hasProp("IntRecordCount"))
            obj->m_IntRecordCount.setown(new Integer64(ptree->getPropInt64("IntRecordCount")));

        if (ptree->hasProp("FromRoxieCluster"))
            obj->m_FromRoxieCluster.setown(new Boolean(ptree->getPropBool("FromRoxieCluster")));

        if (ptree->hasProp("BrowseData"))
            obj->m_BrowseData.setown(new Boolean(ptree->getPropBool("BrowseData")));

        if (ptree->hasProp("IsKeyFile"))
            obj->m_IsKeyFile.setown(new Boolean(ptree->getPropBool("IsKeyFile")));

        if (ptree->hasProp("IsCompressed"))
            obj->m_IsCompressed.setown(new Boolean(ptree->getPropBool("IsCompressed")));

        if (ptree->hasProp("ContentType"))
            obj->m_ContentType.setown(new PString(ptree->queryProp("ContentType")));

        if (ptree->hasProp("CompressedFileSize"))
            obj->m_CompressedFileSize.setown(new Integer64(ptree->getPropInt64("CompressedFileSize")));

        if (ptree->hasProp("SuperOwners"))
            obj->m_SuperOwners.setown(new PString(ptree->queryProp("SuperOwners")));

        if (ptree->hasProp("Persistent"))
            obj->m_Persistent.setown(new Boolean(ptree->getPropBool("Persistent")));

        if (ptree->hasProp("IsProtected"))
            obj->m_IsProtected.setown(new Boolean(ptree->getPropBool("IsProtected")));

        if (ptree->hasProp("KeyType"))
            obj->m_KeyType.setown(new PString(ptree->queryProp("KeyType")));

        if (ptree->hasProp("NumOfSubfiles"))
            obj->m_NumOfSubfiles.setown(new Integer(ptree->getPropInt("NumOfSubfiles")));

        if (ptree->hasProp("Accessed"))
            obj->m_Accessed.setown(new PString(ptree->queryProp("Accessed")));

        return 0;
    }

    int unserialize(DFUFileViewResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Scope"))
            obj->m_Scope.setown(new PString(ptree->queryProp("Scope")));

        if (ptree->hasProp("NumFiles"))
            obj->m_NumFiles.setown(new Integer(ptree->getPropInt("NumFiles")));

        subtree = ptree->queryPropTree("DFULogicalFiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFULogicalFile> oneobj(new DFULogicalFile());
                unserialize(oneobj.get(), onetree);
                obj->m_DFULogicalFiles.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(DFUGetDataColumnsRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("OpenLogicalName"))
            obj->m_OpenLogicalName.setown(new PString(ptree->queryProp("OpenLogicalName")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("FilterBy"))
            obj->m_FilterBy.setown(new PString(ptree->queryProp("FilterBy")));

        if (ptree->hasProp("ShowColumns"))
            obj->m_ShowColumns.setown(new PString(ptree->queryProp("ShowColumns")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        if (ptree->hasProp("StartIndex"))
            obj->m_StartIndex.setown(new Integer64(ptree->getPropInt64("StartIndex")));

        if (ptree->hasProp("EndIndex"))
            obj->m_EndIndex.setown(new Integer64(ptree->getPropInt64("EndIndex")));

        return 0;
    }

    int unserialize(DFUGetDataColumnsResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("StartIndex"))
            obj->m_StartIndex.setown(new Integer64(ptree->getPropInt64("StartIndex")));

        if (ptree->hasProp("EndIndex"))
            obj->m_EndIndex.setown(new Integer64(ptree->getPropInt64("EndIndex")));

        subtree = ptree->queryPropTree("DFUDataKeyedColumns1");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns1.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns2");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns2.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns3");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns3.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns4");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns4.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns5");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns5.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns6");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns6.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns7");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns7.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns8");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns8.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns9");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns9.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns10");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns10.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns11");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns11.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns12");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns12.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns13");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns13.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns14");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns14.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns15");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns15.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns16");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns16.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns17");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns17.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns18");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns18.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns19");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns19.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns20");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns20.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns1");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns1.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns2");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns2.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns3");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns3.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns4");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns4.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns5");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns5.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns6");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns6.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns7");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns7.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns8");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns8.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns9");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns9.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns10");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns10.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns11");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns11.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns12");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns12.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns13");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns13.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns14");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns14.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns15");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns15.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns16");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns16.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns17");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns17.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns18");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns18.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns19");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns19.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns20");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns20.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("RowCount"))
            obj->m_RowCount.setown(new Integer64(ptree->getPropInt64("RowCount")));

        if (ptree->hasProp("ShowColumns"))
            obj->m_ShowColumns.setown(new PString(ptree->queryProp("ShowColumns")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        return 0;
    }

    int unserialize(DFUGetFileMetaDataRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("LogicalFileName"))
            obj->m_LogicalFileName.setown(new PString(ptree->queryProp("LogicalFileName")));

        if (ptree->hasProp("ClusterName"))
            obj->m_ClusterName.setown(new PString(ptree->queryProp("ClusterName")));

        if (ptree->hasProp("IncludeXmlSchema"))
            obj->m_IncludeXmlSchema.setown(new Boolean(ptree->getPropBool("IncludeXmlSchema")));

        if (ptree->hasProp("AddHeaderInXmlSchema"))
            obj->m_AddHeaderInXmlSchema.setown(new Boolean(ptree->getPropBool("AddHeaderInXmlSchema")));

        if (ptree->hasProp("IncludeXmlXPathSchema"))
            obj->m_IncludeXmlXPathSchema.setown(new Boolean(ptree->getPropBool("IncludeXmlXPathSchema")));

        if (ptree->hasProp("AddHeaderInXmlXPathSchema"))
            obj->m_AddHeaderInXmlXPathSchema.setown(new Boolean(ptree->getPropBool("AddHeaderInXmlXPathSchema")));

        return 0;
    }

    int unserialize(DFUGetFileMetaDataResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("TotalColumnCount"))
            obj->m_TotalColumnCount.setown(new Integer(ptree->getPropInt("TotalColumnCount")));

        if (ptree->hasProp("KeyedColumnCount"))
            obj->m_KeyedColumnCount.setown(new Integer(ptree->getPropInt("KeyedColumnCount")));

        subtree = ptree->queryPropTree("DataColumns");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DataColumns.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("XmlSchema"))
            obj->m_XmlSchema.setown(new PString(ptree->queryProp("XmlSchema")));

        if (ptree->hasProp("XmlXPathSchema"))
            obj->m_XmlXPathSchema.setown(new PString(ptree->queryProp("XmlXPathSchema")));

        if (ptree->hasProp("TotalResultRows"))
            obj->m_TotalResultRows.setown(new Integer64(ptree->getPropInt64("TotalResultRows")));

        return 0;
    }

    int unserialize(DFUInfoRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("UpdateDescription"))
            obj->m_UpdateDescription.setown(new Boolean(ptree->getPropBool("UpdateDescription")));

        if (ptree->hasProp("QuerySet"))
            obj->m_QuerySet.setown(new PString(ptree->queryProp("QuerySet")));

        if (ptree->hasProp("Query"))
            obj->m_Query.setown(new PString(ptree->queryProp("Query")));

        if (ptree->hasProp("FileName"))
            obj->m_FileName.setown(new PString(ptree->queryProp("FileName")));

        if (ptree->hasProp("FileDesc"))
            obj->m_FileDesc.setown(new PString(ptree->queryProp("FileDesc")));

        if (ptree->hasProp("IncludeJsonTypeInfo"))
            obj->m_IncludeJsonTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeJsonTypeInfo")));

        if (ptree->hasProp("IncludeBinTypeInfo"))
            obj->m_IncludeBinTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeBinTypeInfo")));

        if (ptree->hasProp("Protect"))
            obj->m_Protect = EnumHandlerDFUChangeProtection::fromString(ptree->queryProp("Protect"));

        if (ptree->hasProp("Restrict"))
            obj->m_Restrict = EnumHandlerDFUChangeRestriction::fromString(ptree->queryProp("Restrict"));

        return 0;
    }

    int unserialize(DFUFileStat* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("MinSkew"))
            obj->m_MinSkew.setown(new PString(ptree->queryProp("MinSkew")));

        if (ptree->hasProp("MaxSkew"))
            obj->m_MaxSkew.setown(new PString(ptree->queryProp("MaxSkew")));

        if (ptree->hasProp("MinSkewInt64"))
            obj->m_MinSkewInt64.setown(new Integer64(ptree->getPropInt64("MinSkewInt64")));

        if (ptree->hasProp("MaxSkewInt64"))
            obj->m_MaxSkewInt64.setown(new Integer64(ptree->getPropInt64("MaxSkewInt64")));

        if (ptree->hasProp("MinSkewPart"))
            obj->m_MinSkewPart.setown(new Integer64(ptree->getPropInt64("MinSkewPart")));

        if (ptree->hasProp("MaxSkewPart"))
            obj->m_MaxSkewPart.setown(new Integer64(ptree->getPropInt64("MaxSkewPart")));

        return 0;
    }

    int unserialize(DFUPart* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Id"))
            obj->m_Id.setown(new Integer(ptree->getPropInt("Id")));

        if (ptree->hasProp("Copy"))
            obj->m_Copy.setown(new Integer(ptree->getPropInt("Copy")));

        if (ptree->hasProp("ActualSize"))
            obj->m_ActualSize.setown(new PString(ptree->queryProp("ActualSize")));

        if (ptree->hasProp("Ip"))
            obj->m_Ip.setown(new PString(ptree->queryProp("Ip")));

        if (ptree->hasProp("Partsize"))
            obj->m_Partsize.setown(new PString(ptree->queryProp("Partsize")));

        if (ptree->hasProp("PartSizeInt64"))
            obj->m_PartSizeInt64.setown(new Integer64(ptree->getPropInt64("PartSizeInt64")));

        return 0;
    }

    int unserialize(DFUFilePartsOnCluster* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("BaseDir"))
            obj->m_BaseDir.setown(new PString(ptree->queryProp("BaseDir")));

        if (ptree->hasProp("ReplicateDir"))
            obj->m_ReplicateDir.setown(new PString(ptree->queryProp("ReplicateDir")));

        if (ptree->hasProp("Replicate"))
            obj->m_Replicate.setown(new Boolean(ptree->getPropBool("Replicate")));

        if (ptree->hasProp("CanReplicate"))
            obj->m_CanReplicate.setown(new Boolean(ptree->getPropBool("CanReplicate")));

        subtree = ptree->queryPropTree("DFUFileParts");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUPart> oneobj(new DFUPart());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUFileParts.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(DFUFileProtect* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("Count"))
            obj->m_Count.setown(new Integer(ptree->getPropInt("Count")));

        if (ptree->hasProp("Modified"))
            obj->m_Modified.setown(new PString(ptree->queryProp("Modified")));

        return 0;
    }

    int unserialize(DFUFilePartition* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("FieldMask"))
            obj->m_FieldMask.setown(new Integer64(ptree->getPropInt64("FieldMask")));

        subtree = ptree->queryPropTree("FieldNames");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_FieldNames.append(*(new PString(onetree->queryProp("."))));

            }

         }

        return 0;
    }

    int unserialize(DFUFileBloom* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("FieldMask"))
            obj->m_FieldMask.setown(new Integer64(ptree->getPropInt64("FieldMask")));

        subtree = ptree->queryPropTree("FieldNames");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_FieldNames.append(*(new PString(onetree->queryProp("."))));

            }

         }

        if (ptree->hasProp("Limit"))
            obj->m_Limit.setown(new Integer64(ptree->getPropInt64("Limit")));

        if (ptree->hasProp("Probability"))
            obj->m_Probability.setown(new PString(ptree->queryProp("Probability")));

        return 0;
    }

    int unserialize(DFUFileDetail* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Filename"))
            obj->m_Filename.setown(new PString(ptree->queryProp("Filename")));

        if (ptree->hasProp("Prefix"))
            obj->m_Prefix.setown(new PString(ptree->queryProp("Prefix")));

        if (ptree->hasProp("NodeGroup"))
            obj->m_NodeGroup.setown(new PString(ptree->queryProp("NodeGroup")));

        if (ptree->hasProp("NumParts"))
            obj->m_NumParts.setown(new Integer(ptree->getPropInt("NumParts")));

        if (ptree->hasProp("Description"))
            obj->m_Description.setown(new PString(ptree->queryProp("Description")));

        if (ptree->hasProp("Dir"))
            obj->m_Dir.setown(new PString(ptree->queryProp("Dir")));

        if (ptree->hasProp("PathMask"))
            obj->m_PathMask.setown(new PString(ptree->queryProp("PathMask")));

        if (ptree->hasProp("Filesize"))
            obj->m_Filesize.setown(new PString(ptree->queryProp("Filesize")));

        if (ptree->hasProp("FileSizeInt64"))
            obj->m_FileSizeInt64.setown(new Integer64(ptree->getPropInt64("FileSizeInt64")));

        if (ptree->hasProp("ActualSize"))
            obj->m_ActualSize.setown(new PString(ptree->queryProp("ActualSize")));

        if (ptree->hasProp("RecordSize"))
            obj->m_RecordSize.setown(new PString(ptree->queryProp("RecordSize")));

        if (ptree->hasProp("RecordCount"))
            obj->m_RecordCount.setown(new PString(ptree->queryProp("RecordCount")));

        if (ptree->hasProp("RecordSizeInt64"))
            obj->m_RecordSizeInt64.setown(new Integer64(ptree->getPropInt64("RecordSizeInt64")));

        if (ptree->hasProp("RecordCountInt64"))
            obj->m_RecordCountInt64.setown(new Integer64(ptree->getPropInt64("RecordCountInt64")));

        if (ptree->hasProp("Wuid"))
            obj->m_Wuid.setown(new PString(ptree->queryProp("Wuid")));

        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("JobName"))
            obj->m_JobName.setown(new PString(ptree->queryProp("JobName")));

        if (ptree->hasProp("Persistent"))
            obj->m_Persistent.setown(new PString(ptree->queryProp("Persistent")));

        if (ptree->hasProp("Format"))
            obj->m_Format.setown(new PString(ptree->queryProp("Format")));

        if (ptree->hasProp("MaxRecordSize"))
            obj->m_MaxRecordSize.setown(new PString(ptree->queryProp("MaxRecordSize")));

        if (ptree->hasProp("CsvSeparate"))
            obj->m_CsvSeparate.setown(new PString(ptree->queryProp("CsvSeparate")));

        if (ptree->hasProp("CsvQuote"))
            obj->m_CsvQuote.setown(new PString(ptree->queryProp("CsvQuote")));

        if (ptree->hasProp("CsvTerminate"))
            obj->m_CsvTerminate.setown(new PString(ptree->queryProp("CsvTerminate")));

        if (ptree->hasProp("CsvEscape"))
            obj->m_CsvEscape.setown(new PString(ptree->queryProp("CsvEscape")));

        if (ptree->hasProp("Modified"))
            obj->m_Modified.setown(new PString(ptree->queryProp("Modified")));

        if (ptree->hasProp("Ecl"))
            obj->m_Ecl.setown(new PString(ptree->queryProp("Ecl")));

        if (ptree->hasProp("ZipFile"))
            obj->m_ZipFile.setown(new Boolean(ptree->getPropBool("ZipFile")));

        subtree = ptree->queryPropTree("Stat");
        if (subtree != nullptr)
        {
            obj->m_Stat.setown(new DFUFileStat());
            unserialize(obj->m_Stat.get(), subtree);
        }

        subtree = ptree->queryPropTree("DFUFileParts");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUPart> oneobj(new DFUPart());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUFileParts.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUFilePartsOnClusters");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUFilePartsOnCluster> oneobj(new DFUFilePartsOnCluster());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUFilePartsOnClusters.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("isSuperfile"))
            obj->m_isSuperfile.setown(new Boolean(ptree->getPropBool("isSuperfile")));

        if (ptree->hasProp("ShowFileContent"))
            obj->m_ShowFileContent.setown(new Boolean(ptree->getPropBool("ShowFileContent")));

        subtree = ptree->queryPropTree("subfiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_subfiles.append(*(new PString(onetree->queryProp("."))));

            }

         }

        subtree = ptree->queryPropTree("Superfiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFULogicalFile> oneobj(new DFULogicalFile());
                unserialize(oneobj.get(), onetree);
                obj->m_Superfiles.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("ProtectList");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUFileProtect> oneobj(new DFUFileProtect());
                unserialize(oneobj.get(), onetree);
                obj->m_ProtectList.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("FromRoxieCluster"))
            obj->m_FromRoxieCluster.setown(new Boolean(ptree->getPropBool("FromRoxieCluster")));

        subtree = ptree->queryPropTree("Graphs");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("ECLGraph");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_Graphs.append(*(new PString(onetree->queryProp("."))));

            }

         }

        if (ptree->hasProp("UserPermission"))
            obj->m_UserPermission.setown(new PString(ptree->queryProp("UserPermission")));

        if (ptree->hasProp("ContentType"))
            obj->m_ContentType.setown(new PString(ptree->queryProp("ContentType")));

        if (ptree->hasProp("CompressedFileSize"))
            obj->m_CompressedFileSize.setown(new Integer64(ptree->getPropInt64("CompressedFileSize")));

        if (ptree->hasProp("PercentCompressed"))
            obj->m_PercentCompressed.setown(new PString(ptree->queryProp("PercentCompressed")));

        if (ptree->hasProp("IsCompressed"))
            obj->m_IsCompressed.setown(new Boolean(ptree->getPropBool("IsCompressed")));

        if (ptree->hasProp("IsRestricted"))
            obj->m_IsRestricted.setown(new Boolean(ptree->getPropBool("IsRestricted")));

        if (ptree->hasProp("BrowseData"))
            obj->m_BrowseData.setown(new Boolean(ptree->getPropBool("BrowseData")));

        if (ptree->hasProp("jsonInfo"))
            obj->m_jsonInfo.setown(new PString(ptree->queryProp("jsonInfo")));

        if (ptree->hasProp("binInfo"))
            obj->m_binInfo.setown(new PString(ptree->queryProp("binInfo")));

        if (ptree->hasProp("PackageID"))
            obj->m_PackageID.setown(new PString(ptree->queryProp("PackageID")));

        subtree = ptree->queryPropTree("Partition");
        if (subtree != nullptr)
        {
            obj->m_Partition.setown(new DFUFilePartition());
            unserialize(obj->m_Partition.get(), subtree);
        }

        subtree = ptree->queryPropTree("Blooms");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUFileBloom> oneobj(new DFUFileBloom());
                unserialize(oneobj.get(), onetree);
                obj->m_Blooms.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("ExpireDays"))
            obj->m_ExpireDays.setown(new Integer(ptree->getPropInt("ExpireDays")));

        if (ptree->hasProp("KeyType"))
            obj->m_KeyType.setown(new PString(ptree->queryProp("KeyType")));

        return 0;
    }

    int unserialize(DFUInfoResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        subtree = ptree->queryPropTree("FileDetail");
        if (subtree != nullptr)
        {
            obj->m_FileDetail.setown(new DFUFileDetail());
            unserialize(obj->m_FileDetail.get(), subtree);
        }

        return 0;
    }

    int unserialize(DFUQueryRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Prefix"))
            obj->m_Prefix.setown(new PString(ptree->queryProp("Prefix")));

        if (ptree->hasProp("ClusterName"))
            obj->m_ClusterName.setown(new PString(ptree->queryProp("ClusterName")));

        if (ptree->hasProp("NodeGroup"))
            obj->m_NodeGroup.setown(new PString(ptree->queryProp("NodeGroup")));

        if (ptree->hasProp("ContentType"))
            obj->m_ContentType.setown(new PString(ptree->queryProp("ContentType")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("StartDate"))
            obj->m_StartDate.setown(new PString(ptree->queryProp("StartDate")));

        if (ptree->hasProp("EndDate"))
            obj->m_EndDate.setown(new PString(ptree->queryProp("EndDate")));

        if (ptree->hasProp("FileType"))
            obj->m_FileType.setown(new PString(ptree->queryProp("FileType")));

        if (ptree->hasProp("FileSizeFrom"))
            obj->m_FileSizeFrom.setown(new Integer64(ptree->getPropInt64("FileSizeFrom")));

        if (ptree->hasProp("FileSizeTo"))
            obj->m_FileSizeTo.setown(new Integer64(ptree->getPropInt64("FileSizeTo")));

        if (ptree->hasProp("FirstN"))
            obj->m_FirstN.setown(new Integer(ptree->getPropInt("FirstN")));

        if (ptree->hasProp("FirstNType"))
            obj->m_FirstNType.setown(new PString(ptree->queryProp("FirstNType")));

        if (ptree->hasProp("PageSize"))
            obj->m_PageSize.setown(new Integer(ptree->getPropInt("PageSize")));

        if (ptree->hasProp("PageStartFrom"))
            obj->m_PageStartFrom.setown(new Integer(ptree->getPropInt("PageStartFrom")));

        if (ptree->hasProp("Sortby"))
            obj->m_Sortby.setown(new PString(ptree->queryProp("Sortby")));

        if (ptree->hasProp("Descending"))
            obj->m_Descending.setown(new Boolean(ptree->getPropBool("Descending")));

        if (ptree->hasProp("OneLevelDirFileReturn"))
            obj->m_OneLevelDirFileReturn.setown(new Boolean(ptree->getPropBool("OneLevelDirFileReturn")));

        if (ptree->hasProp("CacheHint"))
            obj->m_CacheHint.setown(new Integer64(ptree->getPropInt64("CacheHint")));

        if (ptree->hasProp("MaxNumberOfFiles"))
            obj->m_MaxNumberOfFiles.setown(new Integer(ptree->getPropInt("MaxNumberOfFiles")));

        if (ptree->hasProp("IncludeSuperOwner"))
            obj->m_IncludeSuperOwner.setown(new Boolean(ptree->getPropBool("IncludeSuperOwner")));

        if (ptree->hasProp("StartAccessedTime"))
            obj->m_StartAccessedTime.setown(new PString(ptree->queryProp("StartAccessedTime")));

        if (ptree->hasProp("EndAccessedTime"))
            obj->m_EndAccessedTime.setown(new PString(ptree->queryProp("EndAccessedTime")));

        return 0;
    }

    int unserialize(DFUQueryResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        subtree = ptree->queryPropTree("DFULogicalFiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFULogicalFile> oneobj(new DFULogicalFile());
                unserialize(oneobj.get(), onetree);
                obj->m_DFULogicalFiles.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("Prefix"))
            obj->m_Prefix.setown(new PString(ptree->queryProp("Prefix")));

        if (ptree->hasProp("ClusterName"))
            obj->m_ClusterName.setown(new PString(ptree->queryProp("ClusterName")));

        if (ptree->hasProp("NodeGroup"))
            obj->m_NodeGroup.setown(new PString(ptree->queryProp("NodeGroup")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("Description"))
            obj->m_Description.setown(new PString(ptree->queryProp("Description")));

        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("StartDate"))
            obj->m_StartDate.setown(new PString(ptree->queryProp("StartDate")));

        if (ptree->hasProp("EndDate"))
            obj->m_EndDate.setown(new PString(ptree->queryProp("EndDate")));

        if (ptree->hasProp("FileType"))
            obj->m_FileType.setown(new PString(ptree->queryProp("FileType")));

        if (ptree->hasProp("FileSizeFrom"))
            obj->m_FileSizeFrom.setown(new Integer64(ptree->getPropInt64("FileSizeFrom")));

        if (ptree->hasProp("FileSizeTo"))
            obj->m_FileSizeTo.setown(new Integer64(ptree->getPropInt64("FileSizeTo")));

        if (ptree->hasProp("FirstN"))
            obj->m_FirstN.setown(new Integer(ptree->getPropInt("FirstN")));

        if (ptree->hasProp("FirstNType"))
            obj->m_FirstNType.setown(new PString(ptree->queryProp("FirstNType")));

        if (ptree->hasProp("PageSize"))
            obj->m_PageSize.setown(new Integer(ptree->getPropInt("PageSize")));

        if (ptree->hasProp("PageStartFrom"))
            obj->m_PageStartFrom.setown(new Integer64(ptree->getPropInt64("PageStartFrom")));

        if (ptree->hasProp("LastPageFrom"))
            obj->m_LastPageFrom.setown(new Integer64(ptree->getPropInt64("LastPageFrom")));

        if (ptree->hasProp("PageEndAt"))
            obj->m_PageEndAt.setown(new Integer64(ptree->getPropInt64("PageEndAt")));

        if (ptree->hasProp("PrevPageFrom"))
            obj->m_PrevPageFrom.setown(new Integer64(ptree->getPropInt64("PrevPageFrom")));

        if (ptree->hasProp("NextPageFrom"))
            obj->m_NextPageFrom.setown(new Integer64(ptree->getPropInt64("NextPageFrom")));

        if (ptree->hasProp("NumFiles"))
            obj->m_NumFiles.setown(new Integer64(ptree->getPropInt64("NumFiles")));

        if (ptree->hasProp("Sortby"))
            obj->m_Sortby.setown(new PString(ptree->queryProp("Sortby")));

        if (ptree->hasProp("Descending"))
            obj->m_Descending.setown(new Boolean(ptree->getPropBool("Descending")));

        if (ptree->hasProp("BasicQuery"))
            obj->m_BasicQuery.setown(new PString(ptree->queryProp("BasicQuery")));

        if (ptree->hasProp("ParametersForPaging"))
            obj->m_ParametersForPaging.setown(new PString(ptree->queryProp("ParametersForPaging")));

        if (ptree->hasProp("Filters"))
            obj->m_Filters.setown(new PString(ptree->queryProp("Filters")));

        if (ptree->hasProp("CacheHint"))
            obj->m_CacheHint.setown(new Integer64(ptree->getPropInt64("CacheHint")));

        if (ptree->hasProp("IsSubsetOfFiles"))
            obj->m_IsSubsetOfFiles.setown(new Boolean(ptree->getPropBool("IsSubsetOfFiles")));

        if (ptree->hasProp("Warning"))
            obj->m_Warning.setown(new PString(ptree->queryProp("Warning")));

        return 0;
    }

    int unserialize(DFURecordTypeInfoRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("IncludeJsonTypeInfo"))
            obj->m_IncludeJsonTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeJsonTypeInfo")));

        if (ptree->hasProp("IncludeBinTypeInfo"))
            obj->m_IncludeBinTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeBinTypeInfo")));

        return 0;
    }

    int unserialize(DFURecordTypeInfoResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("jsonInfo"))
            obj->m_jsonInfo.setown(new PString(ptree->queryProp("jsonInfo")));

        if (ptree->hasProp("binInfo"))
            obj->m_binInfo.setown(new PString(ptree->queryProp("binInfo")));

        return 0;
    }

    int unserialize(DFUSearchRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ShowExample"))
            obj->m_ShowExample.setown(new PString(ptree->queryProp("ShowExample")));

        return 0;
    }

    int unserialize(DFUSearchResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("ShowExample"))
            obj->m_ShowExample.setown(new PString(ptree->queryProp("ShowExample")));

        subtree = ptree->queryPropTree("ClusterNames");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("ClusterName");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_ClusterNames.append(*(new PString(onetree->queryProp("."))));

            }

         }

        subtree = ptree->queryPropTree("FileTypes");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("FileType");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_FileTypes.append(*(new PString(onetree->queryProp("."))));

            }

         }

        return 0;
    }

    int unserialize(DFUSearchDataRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        if (ptree->hasProp("OpenLogicalName"))
            obj->m_OpenLogicalName.setown(new PString(ptree->queryProp("OpenLogicalName")));

        if (ptree->hasProp("FilterBy"))
            obj->m_FilterBy.setown(new PString(ptree->queryProp("FilterBy")));

        if (ptree->hasProp("ShowColumns"))
            obj->m_ShowColumns.setown(new PString(ptree->queryProp("ShowColumns")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("StartIndex"))
            obj->m_StartIndex.setown(new Integer64(ptree->getPropInt64("StartIndex")));

        if (ptree->hasProp("EndIndex"))
            obj->m_EndIndex.setown(new Integer64(ptree->getPropInt64("EndIndex")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("ParentName"))
            obj->m_ParentName.setown(new PString(ptree->queryProp("ParentName")));

        if (ptree->hasProp("StartForGoback"))
            obj->m_StartForGoback.setown(new Integer64(ptree->getPropInt64("StartForGoback")));

        if (ptree->hasProp("CountForGoback"))
            obj->m_CountForGoback.setown(new Integer(ptree->getPropInt("CountForGoback")));

        if (ptree->hasProp("Start"))
            obj->m_Start.setown(new Integer64(ptree->getPropInt64("Start")));

        if (ptree->hasProp("Count"))
            obj->m_Count.setown(new Integer(ptree->getPropInt("Count")));

        if (ptree->hasProp("File"))
            obj->m_File.setown(new PString(ptree->queryProp("File")));

        if (ptree->hasProp("Key"))
            obj->m_Key.setown(new PString(ptree->queryProp("Key")));

        if (ptree->hasProp("SchemaOnly"))
            obj->m_SchemaOnly.setown(new Boolean(ptree->getPropBool("SchemaOnly")));

        if (ptree->hasProp("RoxieSelections"))
            obj->m_RoxieSelections.setown(new Boolean(ptree->getPropBool("RoxieSelections")));

        if (ptree->hasProp("DisableUppercaseTranslation"))
            obj->m_DisableUppercaseTranslation.setown(new Boolean(ptree->getPropBool("DisableUppercaseTranslation")));

        if (ptree->hasProp("SelectedKey"))
            obj->m_SelectedKey.setown(new PString(ptree->queryProp("SelectedKey")));

        return 0;
    }

    int unserialize(DFUSearchDataResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("OpenLogicalName"))
            obj->m_OpenLogicalName.setown(new PString(ptree->queryProp("OpenLogicalName")));

        if (ptree->hasProp("LogicalName"))
            obj->m_LogicalName.setown(new PString(ptree->queryProp("LogicalName")));

        if (ptree->hasProp("ParentName"))
            obj->m_ParentName.setown(new PString(ptree->queryProp("ParentName")));

        if (ptree->hasProp("StartIndex"))
            obj->m_StartIndex.setown(new Integer64(ptree->getPropInt64("StartIndex")));

        if (ptree->hasProp("EndIndex"))
            obj->m_EndIndex.setown(new Integer64(ptree->getPropInt64("EndIndex")));

        subtree = ptree->queryPropTree("DFUDataKeyedColumns1");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns1.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns2");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns2.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns3");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns3.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns4");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns4.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns5");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns5.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns6");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns6.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns7");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns7.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns8");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns8.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns9");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns9.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns10");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns10.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns11");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns11.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns12");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns12.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns13");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns13.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns14");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns14.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns15");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns15.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns16");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns16.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns17");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns17.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns18");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns18.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns19");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns19.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataKeyedColumns20");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataKeyedColumns20.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns1");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns1.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns2");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns2.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns3");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns3.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns4");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns4.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns5");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns5.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns6");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns6.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns7");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns7.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns8");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns8.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns9");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns9.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns10");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns10.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns11");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns11.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns12");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns12.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns13");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns13.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns14");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns14.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns15");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns15.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns16");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns16.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns17");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns17.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns18");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns18.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns19");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns19.append(*oneobj.getClear());

            }

         }

        subtree = ptree->queryPropTree("DFUDataNonKeyedColumns20");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUDataNonKeyedColumns20.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("RowCount"))
            obj->m_RowCount.setown(new Integer64(ptree->getPropInt64("RowCount")));

        if (ptree->hasProp("ShowColumns"))
            obj->m_ShowColumns.setown(new PString(ptree->queryProp("ShowColumns")));

        if (ptree->hasProp("ChooseFile"))
            obj->m_ChooseFile.setown(new Integer(ptree->getPropInt("ChooseFile")));

        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("FilterBy"))
            obj->m_FilterBy.setown(new PString(ptree->queryProp("FilterBy")));

        if (ptree->hasProp("FilterForGoBack"))
            obj->m_FilterForGoBack.setown(new PString(ptree->queryProp("FilterForGoBack")));

        subtree = ptree->queryPropTree("ColumnsHidden");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("ColumnHidden");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUDataColumn> oneobj(new DFUDataColumn());
                unserialize(oneobj.get(), onetree);
                obj->m_ColumnsHidden.append(*oneobj.getClear());

            }

         }

        if (ptree->hasProp("ColumnCount"))
            obj->m_ColumnCount.setown(new Integer(ptree->getPropInt("ColumnCount")));

        if (ptree->hasProp("StartForGoback"))
            obj->m_StartForGoback.setown(new Integer64(ptree->getPropInt64("StartForGoback")));

        if (ptree->hasProp("CountForGoback"))
            obj->m_CountForGoback.setown(new Integer(ptree->getPropInt("CountForGoback")));

        if (ptree->hasProp("Start"))
            obj->m_Start.setown(new Integer64(ptree->getPropInt64("Start")));

        if (ptree->hasProp("Count"))
            obj->m_Count.setown(new Integer64(ptree->getPropInt64("Count")));

        if (ptree->hasProp("PageSize"))
            obj->m_PageSize.setown(new Integer64(ptree->getPropInt64("PageSize")));

        if (ptree->hasProp("Total"))
            obj->m_Total.setown(new Integer64(ptree->getPropInt64("Total")));

        if (ptree->hasProp("Result"))
            obj->m_Result.setown(new PString(ptree->queryProp("Result")));

        if (ptree->hasProp("MsgToDisplay"))
            obj->m_MsgToDisplay.setown(new PString(ptree->queryProp("MsgToDisplay")));

        if (ptree->hasProp("Cluster"))
            obj->m_Cluster.setown(new PString(ptree->queryProp("Cluster")));

        if (ptree->hasProp("ClusterType"))
            obj->m_ClusterType.setown(new PString(ptree->queryProp("ClusterType")));

        if (ptree->hasProp("File"))
            obj->m_File.setown(new PString(ptree->queryProp("File")));

        if (ptree->hasProp("Key"))
            obj->m_Key.setown(new PString(ptree->queryProp("Key")));

        if (ptree->hasProp("SchemaOnly"))
            obj->m_SchemaOnly.setown(new Boolean(ptree->getPropBool("SchemaOnly")));

        if (ptree->hasProp("RoxieSelections"))
            obj->m_RoxieSelections.setown(new Boolean(ptree->getPropBool("RoxieSelections")));

        if (ptree->hasProp("DisableUppercaseTranslation"))
            obj->m_DisableUppercaseTranslation.setown(new Boolean(ptree->getPropBool("DisableUppercaseTranslation")));

        if (ptree->hasProp("AutoUppercaseTranslation"))
            obj->m_AutoUppercaseTranslation.setown(new Boolean(ptree->getPropBool("AutoUppercaseTranslation")));

        if (ptree->hasProp("SelectedKey"))
            obj->m_SelectedKey.setown(new PString(ptree->queryProp("SelectedKey")));

        return 0;
    }

    int unserialize(DFUSpaceRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("CountBy"))
            obj->m_CountBy.setown(new PString(ptree->queryProp("CountBy")));

        if (ptree->hasProp("ScopeUnder"))
            obj->m_ScopeUnder.setown(new PString(ptree->queryProp("ScopeUnder")));

        if (ptree->hasProp("OwnerUnder"))
            obj->m_OwnerUnder.setown(new PString(ptree->queryProp("OwnerUnder")));

        if (ptree->hasProp("Interval"))
            obj->m_Interval.setown(new PString(ptree->queryProp("Interval")));

        if (ptree->hasProp("StartDate"))
            obj->m_StartDate.setown(new PString(ptree->queryProp("StartDate")));

        if (ptree->hasProp("EndDate"))
            obj->m_EndDate.setown(new PString(ptree->queryProp("EndDate")));

        return 0;
    }

    int unserialize(DFUSpaceItem* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("NumOfFiles"))
            obj->m_NumOfFiles.setown(new PString(ptree->queryProp("NumOfFiles")));

        if (ptree->hasProp("NumOfFilesUnknown"))
            obj->m_NumOfFilesUnknown.setown(new PString(ptree->queryProp("NumOfFilesUnknown")));

        if (ptree->hasProp("TotalSize"))
            obj->m_TotalSize.setown(new PString(ptree->queryProp("TotalSize")));

        if (ptree->hasProp("LargestFile"))
            obj->m_LargestFile.setown(new PString(ptree->queryProp("LargestFile")));

        if (ptree->hasProp("LargestSize"))
            obj->m_LargestSize.setown(new PString(ptree->queryProp("LargestSize")));

        if (ptree->hasProp("SmallestFile"))
            obj->m_SmallestFile.setown(new PString(ptree->queryProp("SmallestFile")));

        if (ptree->hasProp("SmallestSize"))
            obj->m_SmallestSize.setown(new PString(ptree->queryProp("SmallestSize")));

        if (ptree->hasProp("NumOfFilesInt64"))
            obj->m_NumOfFilesInt64.setown(new Integer64(ptree->getPropInt64("NumOfFilesInt64")));

        if (ptree->hasProp("NumOfFilesUnknownInt64"))
            obj->m_NumOfFilesUnknownInt64.setown(new Integer64(ptree->getPropInt64("NumOfFilesUnknownInt64")));

        if (ptree->hasProp("TotalSizeInt64"))
            obj->m_TotalSizeInt64.setown(new Integer64(ptree->getPropInt64("TotalSizeInt64")));

        if (ptree->hasProp("LargestSizeInt64"))
            obj->m_LargestSizeInt64.setown(new Integer64(ptree->getPropInt64("LargestSizeInt64")));

        if (ptree->hasProp("SmallestSizeInt64"))
            obj->m_SmallestSizeInt64.setown(new Integer64(ptree->getPropInt64("SmallestSizeInt64")));

        return 0;
    }

    int unserialize(DFUSpaceResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("CountBy"))
            obj->m_CountBy.setown(new PString(ptree->queryProp("CountBy")));

        if (ptree->hasProp("ScopeUnder"))
            obj->m_ScopeUnder.setown(new PString(ptree->queryProp("ScopeUnder")));

        if (ptree->hasProp("OwnerUnder"))
            obj->m_OwnerUnder.setown(new PString(ptree->queryProp("OwnerUnder")));

        if (ptree->hasProp("Interval"))
            obj->m_Interval.setown(new PString(ptree->queryProp("Interval")));

        if (ptree->hasProp("StartDate"))
            obj->m_StartDate.setown(new PString(ptree->queryProp("StartDate")));

        if (ptree->hasProp("EndDate"))
            obj->m_EndDate.setown(new PString(ptree->queryProp("EndDate")));

        subtree = ptree->queryPropTree("DFUSpaceItems");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<DFUSpaceItem> oneobj(new DFUSpaceItem());
                unserialize(oneobj.get(), onetree);
                obj->m_DFUSpaceItems.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(EclRecordTypeInfoRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Ecl"))
            obj->m_Ecl.setown(new PString(ptree->queryProp("Ecl")));

        if (ptree->hasProp("IncludeJsonTypeInfo"))
            obj->m_IncludeJsonTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeJsonTypeInfo")));

        if (ptree->hasProp("IncludeBinTypeInfo"))
            obj->m_IncludeBinTypeInfo.setown(new Boolean(ptree->getPropBool("IncludeBinTypeInfo")));

        return 0;
    }

    int unserialize(EclRecordTypeInfoResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("jsonInfo"))
            obj->m_jsonInfo.setown(new PString(ptree->queryProp("jsonInfo")));

        if (ptree->hasProp("binInfo"))
            obj->m_binInfo.setown(new PString(ptree->queryProp("binInfo")));

        return 0;
    }

    int unserialize(EraseHistoryRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        return 0;
    }

    int unserialize(History* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        if (ptree->hasProp("Operation"))
            obj->m_Operation.setown(new PString(ptree->queryProp("Operation")));

        if (ptree->hasProp("Timestamp"))
            obj->m_Timestamp.setown(new PString(ptree->queryProp("Timestamp")));

        if (ptree->hasProp("IP"))
            obj->m_IP.setown(new PString(ptree->queryProp("IP")));

        if (ptree->hasProp("Path"))
            obj->m_Path.setown(new PString(ptree->queryProp("Path")));

        if (ptree->hasProp("Owner"))
            obj->m_Owner.setown(new PString(ptree->queryProp("Owner")));

        if (ptree->hasProp("Workunit"))
            obj->m_Workunit.setown(new PString(ptree->queryProp("Workunit")));

        return 0;
    }

    int unserialize(EraseHistoryResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("xmlmap"))
            obj->m_xmlmap.setown(new PString(ptree->queryProp("xmlmap")));

        subtree = ptree->queryPropTree("History");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("Origin");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<History> oneobj(new History());
                unserialize(oneobj.get(), onetree);
                obj->m_History.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(ListHistoryRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("Name"))
            obj->m_Name.setown(new PString(ptree->queryProp("Name")));

        return 0;
    }

    int unserialize(ListHistoryResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("xmlmap"))
            obj->m_xmlmap.setown(new PString(ptree->queryProp("xmlmap")));

        subtree = ptree->queryPropTree("History");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("Origin");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                Owned<History> oneobj(new History());
                unserialize(oneobj.get(), onetree);
                obj->m_History.append(*oneobj.getClear());

            }

         }

        return 0;
    }

    int unserialize(WsDfuPingRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(WsDfuPingResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        return 0;
    }

    int unserialize(SavexmlRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("name"))
            obj->m_name.setown(new PString(ptree->queryProp("name")));

        return 0;
    }

    int unserialize(SavexmlResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("xmlmap"))
            obj->m_xmlmap.setown(new PString(ptree->queryProp("xmlmap")));

        return 0;
    }

    int unserialize(SuperfileActionRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("action"))
            obj->m_action.setown(new PString(ptree->queryProp("action")));

        if (ptree->hasProp("superfile"))
            obj->m_superfile.setown(new PString(ptree->queryProp("superfile")));

        subtree = ptree->queryPropTree("subfiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_subfiles.append(*(new PString(onetree->queryProp("."))));

            }

         }

        if (ptree->hasProp("before"))
            obj->m_before.setown(new PString(ptree->queryProp("before")));

        if (ptree->hasProp("delete"))
            obj->m_delete.setown(new Boolean(ptree->getPropBool("delete")));

        if (ptree->hasProp("removeSuperfile"))
            obj->m_removeSuperfile.setown(new Boolean(ptree->getPropBool("removeSuperfile")));

        return 0;
    }

    int unserialize(SuperfileActionResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("superfile"))
            obj->m_superfile.setown(new PString(ptree->queryProp("superfile")));

        if (ptree->hasProp("retcode"))
            obj->m_retcode.setown(new Integer(ptree->getPropInt("retcode")));

        return 0;
    }

    int unserialize(SuperfileListRequest* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("superfile"))
            obj->m_superfile.setown(new PString(ptree->queryProp("superfile")));

        return 0;
    }

    int unserialize(SuperfileListResponse* obj, IPropertyTree* ptree)
    {
        if (!obj || !ptree)
            return 0;
        IPropertyTree* subtree = nullptr;


        if (ptree->hasProp("superfile"))
            obj->m_superfile.setown(new PString(ptree->queryProp("superfile")));

        subtree = ptree->queryPropTree("subfiles");
        if (subtree != nullptr)
        {
            Owned<IPropertyTreeIterator> itr = subtree->getElements("");
            ForEach (*itr)
            {
                IPropertyTree* onetree = &itr->query();

                obj->m_subfiles.append(*(new PString(onetree->queryProp("."))));

            }

         }

        return 0;
    }

    int serialize(AddRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_dstname)
        {
            buf.append("<dstname>");
            buf.append(obj->m_dstname->str());
            buf.append("</dstname>");
        }

        if (obj->m_xmlmap)
        {
            buf.append("<xmlmap>");
            buf.append(obj->m_xmlmap->str());
            buf.append("</xmlmap>");
        }

        return 0;
    }

    int serialize(AddResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(AddRemoteRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_dstname)
        {
            buf.append("<dstname>");
            buf.append(obj->m_dstname->str());
            buf.append("</dstname>");
        }

        if (obj->m_srcname)
        {
            buf.append("<srcname>");
            buf.append(obj->m_srcname->str());
            buf.append("</srcname>");
        }

        if (obj->m_srcdali)
        {
            buf.append("<srcdali>");
            buf.append(obj->m_srcdali->str());
            buf.append("</srcdali>");
        }

        if (obj->m_srcusername)
        {
            buf.append("<srcusername>");
            buf.append(obj->m_srcusername->str());
            buf.append("</srcusername>");
        }

        if (obj->m_srcpassword)
        {
            buf.append("<srcpassword>");
            buf.append(obj->m_srcpassword->str());
            buf.append("</srcpassword>");
        }

        return 0;
    }

    int serialize(AddRemoteResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(AddtoSuperfileRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Superfile)
        {
            buf.append("<Superfile>");
            buf.append(obj->m_Superfile->str());
            buf.append("</Superfile>");
        }

        if (obj->m_Subfiles)
        {
            buf.append("<Subfiles>");
            buf.append(obj->m_Subfiles->str());
            buf.append("</Subfiles>");
        }

        if (obj->m_names.length() > 0)
        {
            buf.append("<names>");
            ForEachItemIn(i, obj->m_names)
            {
            PString& oneitem = obj->m_names.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</names>");
        }

        if (obj->m_ExistingFile)
        {
            buf.append("<ExistingFile>");
            buf.append(obj->m_ExistingFile->str());
            buf.append("</ExistingFile>");
        }

        if (obj->m_BackToPage)
        {
            buf.append("<BackToPage>");
            buf.append(obj->m_BackToPage->str());
            buf.append("</BackToPage>");
        }

        return 0;
    }

    int serialize(AddtoSuperfileResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Subfiles)
        {
            buf.append("<Subfiles>");
            buf.append(obj->m_Subfiles->str());
            buf.append("</Subfiles>");
        }

        if (obj->m_BackToPage)
        {
            buf.append("<BackToPage>");
            buf.append(obj->m_BackToPage->str());
            buf.append("</BackToPage>");
        }

        if (obj->m_SubfileNames.length() > 0)
        {
            buf.append("<SubfileNames>");
            ForEachItemIn(i, obj->m_SubfileNames)
            {
            PString& oneitem = obj->m_SubfileNames.item(i);
                buf.append("<SubfileName>").append(oneitem.str()).append("</SubfileName>");

            }
            buf.append("</SubfileNames>");
        }

        return 0;
    }

    int serialize(DFUArrayActionRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Type != DFUArrayActions::UNSET)
            buf.append("<Type>").append(EnumHandlerDFUArrayActions::toString(obj->m_Type)).append("</Type>");

        if (obj->m_NoDelete)
        {
            buf.append("<NoDelete>");
            buf.append(obj->m_NoDelete->str());
            buf.append("</NoDelete>");
        }

        if (obj->m_BackToPage)
        {
            buf.append("<BackToPage>");
            buf.append(obj->m_BackToPage->str());
            buf.append("</BackToPage>");
        }

        if (obj->m_LogicalFiles.length() > 0)
        {
            buf.append("<LogicalFiles>");
            ForEachItemIn(i, obj->m_LogicalFiles)
            {
            PString& oneitem = obj->m_LogicalFiles.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</LogicalFiles>");
        }

        if (obj->m_removeFromSuperfiles)
        {
            buf.append("<removeFromSuperfiles>");
            buf.append(obj->m_removeFromSuperfiles->str());
            buf.append("</removeFromSuperfiles>");
        }

        if (obj->m_removeRecursively)
        {
            buf.append("<removeRecursively>");
            buf.append(obj->m_removeRecursively->str());
            buf.append("</removeRecursively>");
        }

        if (obj->m_Protect != DFUChangeProtection::UNSET)
            buf.append("<Protect>").append(EnumHandlerDFUChangeProtection::toString(obj->m_Protect)).append("</Protect>");

        if (obj->m_Restrict != DFUChangeRestriction::UNSET)
            buf.append("<Restrict>").append(EnumHandlerDFUChangeRestriction::toString(obj->m_Restrict)).append("</Restrict>");

        return 0;
    }

    int serialize(DFUActionInfo* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FileName)
        {
            buf.append("<FileName>");
            buf.append(obj->m_FileName->str());
            buf.append("</FileName>");
        }

        if (obj->m_NodeGroup)
        {
            buf.append("<NodeGroup>");
            buf.append(obj->m_NodeGroup->str());
            buf.append("</NodeGroup>");
        }

        if (obj->m_ActionResult)
        {
            buf.append("<ActionResult>");
            buf.append(obj->m_ActionResult->str());
            buf.append("</ActionResult>");
        }

        if (obj->m_Failed)
        {
            buf.append("<Failed>");
            buf.append(obj->m_Failed->str());
            buf.append("</Failed>");
        }

        return 0;
    }

    int serialize(DFUArrayActionResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_BackToPage)
        {
            buf.append("<BackToPage>");
            buf.append(obj->m_BackToPage->str());
            buf.append("</BackToPage>");
        }

        if (obj->m_RedirectTo)
        {
            buf.append("<RedirectTo>");
            buf.append(obj->m_RedirectTo->str());
            buf.append("</RedirectTo>");
        }

        if (obj->m_DFUArrayActionResult)
        {
            buf.append("<DFUArrayActionResult>");
            buf.append(obj->m_DFUArrayActionResult->str());
            buf.append("</DFUArrayActionResult>");
        }

        if (obj->m_ActionResults.length() > 0)
        {
            buf.append("<ActionResults>");
            ForEachItemIn(i, obj->m_ActionResults)
            {
                    DFUActionInfo& oneitem = obj->m_ActionResults.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</ActionResults>");
        }

        return 0;
    }

    int serialize(DFUBrowseDataRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_FilterBy)
        {
            buf.append("<FilterBy>");
            buf.append(obj->m_FilterBy->str());
            buf.append("</FilterBy>");
        }

        if (obj->m_ShowColumns)
        {
            buf.append("<ShowColumns>");
            buf.append(obj->m_ShowColumns->str());
            buf.append("</ShowColumns>");
        }

        if (obj->m_SchemaOnly)
        {
            buf.append("<SchemaOnly>");
            buf.append(obj->m_SchemaOnly->str());
            buf.append("</SchemaOnly>");
        }

        if (obj->m_StartForGoback)
        {
            buf.append("<StartForGoback>");
            buf.append(obj->m_StartForGoback->str());
            buf.append("</StartForGoback>");
        }

        if (obj->m_CountForGoback)
        {
            buf.append("<CountForGoback>");
            buf.append(obj->m_CountForGoback->str());
            buf.append("</CountForGoback>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        if (obj->m_ParentName)
        {
            buf.append("<ParentName>");
            buf.append(obj->m_ParentName->str());
            buf.append("</ParentName>");
        }

        if (obj->m_Start)
        {
            buf.append("<Start>");
            buf.append(obj->m_Start->str());
            buf.append("</Start>");
        }

        if (obj->m_Count)
        {
            buf.append("<Count>");
            buf.append(obj->m_Count->str());
            buf.append("</Count>");
        }

        if (obj->m_DisableUppercaseTranslation)
        {
            buf.append("<DisableUppercaseTranslation>");
            buf.append(obj->m_DisableUppercaseTranslation->str());
            buf.append("</DisableUppercaseTranslation>");
        }

        return 0;
    }

    int serialize(DFUDataColumn* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ColumnID)
        {
            buf.append("<ColumnID>");
            buf.append(obj->m_ColumnID->str());
            buf.append("</ColumnID>");
        }

        if (obj->m_ColumnLabel)
        {
            buf.append("<ColumnLabel>");
            buf.append(obj->m_ColumnLabel->str());
            buf.append("</ColumnLabel>");
        }

        if (obj->m_ColumnType)
        {
            buf.append("<ColumnType>");
            buf.append(obj->m_ColumnType->str());
            buf.append("</ColumnType>");
        }

        if (obj->m_ColumnValue)
        {
            buf.append("<ColumnValue>");
            buf.append(obj->m_ColumnValue->str());
            buf.append("</ColumnValue>");
        }

        if (obj->m_ColumnSize)
        {
            buf.append("<ColumnSize>");
            buf.append(obj->m_ColumnSize->str());
            buf.append("</ColumnSize>");
        }

        if (obj->m_MaxSize)
        {
            buf.append("<MaxSize>");
            buf.append(obj->m_MaxSize->str());
            buf.append("</MaxSize>");
        }

        if (obj->m_ColumnEclType)
        {
            buf.append("<ColumnEclType>");
            buf.append(obj->m_ColumnEclType->str());
            buf.append("</ColumnEclType>");
        }

        if (obj->m_ColumnRawSize)
        {
            buf.append("<ColumnRawSize>");
            buf.append(obj->m_ColumnRawSize->str());
            buf.append("</ColumnRawSize>");
        }

        if (obj->m_IsNaturalColumn)
        {
            buf.append("<IsNaturalColumn>");
            buf.append(obj->m_IsNaturalColumn->str());
            buf.append("</IsNaturalColumn>");
        }

        if (obj->m_IsKeyedColumn)
        {
            buf.append("<IsKeyedColumn>");
            buf.append(obj->m_IsKeyedColumn->str());
            buf.append("</IsKeyedColumn>");
        }

        if (obj->m_DataColumns.length() > 0)
        {
            buf.append("<DataColumns>");
            ForEachItemIn(i, obj->m_DataColumns)
            {
                    DFUDataColumn& oneitem = obj->m_DataColumns.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DataColumns>");
        }

        return 0;
    }

    int serialize(DFUBrowseDataResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_FilterBy)
        {
            buf.append("<FilterBy>");
            buf.append(obj->m_FilterBy->str());
            buf.append("</FilterBy>");
        }

        if (obj->m_FilterForGoBack)
        {
            buf.append("<FilterForGoBack>");
            buf.append(obj->m_FilterForGoBack->str());
            buf.append("</FilterForGoBack>");
        }

        if (obj->m_ColumnsHidden.length() > 0)
        {
            buf.append("<ColumnsHidden>");
            ForEachItemIn(i, obj->m_ColumnsHidden)
            {
                    DFUDataColumn& oneitem = obj->m_ColumnsHidden.item(i);
                buf.append("<ColumnHidden>");
                serialize(&oneitem, buf);
                buf.append("</ColumnHidden>");

            }
            buf.append("</ColumnsHidden>");
        }

        if (obj->m_ColumnCount)
        {
            buf.append("<ColumnCount>");
            buf.append(obj->m_ColumnCount->str());
            buf.append("</ColumnCount>");
        }

        if (obj->m_StartForGoback)
        {
            buf.append("<StartForGoback>");
            buf.append(obj->m_StartForGoback->str());
            buf.append("</StartForGoback>");
        }

        if (obj->m_CountForGoback)
        {
            buf.append("<CountForGoback>");
            buf.append(obj->m_CountForGoback->str());
            buf.append("</CountForGoback>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_SchemaOnly)
        {
            buf.append("<SchemaOnly>");
            buf.append(obj->m_SchemaOnly->str());
            buf.append("</SchemaOnly>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        if (obj->m_ParentName)
        {
            buf.append("<ParentName>");
            buf.append(obj->m_ParentName->str());
            buf.append("</ParentName>");
        }

        if (obj->m_Start)
        {
            buf.append("<Start>");
            buf.append(obj->m_Start->str());
            buf.append("</Start>");
        }

        if (obj->m_Count)
        {
            buf.append("<Count>");
            buf.append(obj->m_Count->str());
            buf.append("</Count>");
        }

        if (obj->m_PageSize)
        {
            buf.append("<PageSize>");
            buf.append(obj->m_PageSize->str());
            buf.append("</PageSize>");
        }

        if (obj->m_Total)
        {
            buf.append("<Total>");
            buf.append(obj->m_Total->str());
            buf.append("</Total>");
        }

        if (obj->m_Result)
        {
            buf.append("<Result>");
            buf.append(obj->m_Result->str());
            buf.append("</Result>");
        }

        if (obj->m_MsgToDisplay)
        {
            buf.append("<MsgToDisplay>");
            buf.append(obj->m_MsgToDisplay->str());
            buf.append("</MsgToDisplay>");
        }

        if (obj->m_DisableUppercaseTranslation)
        {
            buf.append("<DisableUppercaseTranslation>");
            buf.append(obj->m_DisableUppercaseTranslation->str());
            buf.append("</DisableUppercaseTranslation>");
        }

        return 0;
    }

    int serialize(DFUDefFileRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Format != DFUDefFileFormat::UNSET)
            buf.append("<Format>").append(EnumHandlerDFUDefFileFormat::toString(obj->m_Format)).append("</Format>");

        return 0;
    }

    int serialize(DFUDefFileResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_defFile)
        {
            buf.append("<defFile>");
            buf.append(obj->m_defFile->str());
            buf.append("</defFile>");
        }

        return 0;
    }

    int serialize(DFUFileAccessRequestBase* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_JobId)
        {
            buf.append("<JobId>");
            buf.append(obj->m_JobId->str());
            buf.append("</JobId>");
        }

        if (obj->m_ExpirySeconds)
        {
            buf.append("<ExpirySeconds>");
            buf.append(obj->m_ExpirySeconds->str());
            buf.append("</ExpirySeconds>");
        }

        if (obj->m_AccessRole != FileAccessRole::UNSET)
            buf.append("<AccessRole>").append(EnumHandlerFileAccessRole::toString(obj->m_AccessRole)).append("</AccessRole>");

        if (obj->m_AccessType != SecAccessType::UNSET)
            buf.append("<AccessType>").append(EnumHandlerSecAccessType::toString(obj->m_AccessType)).append("</AccessType>");

        if (obj->m_ReturnJsonTypeInfo)
        {
            buf.append("<ReturnJsonTypeInfo>");
            buf.append(obj->m_ReturnJsonTypeInfo->str());
            buf.append("</ReturnJsonTypeInfo>");
        }

        if (obj->m_ReturnBinTypeInfo)
        {
            buf.append("<ReturnBinTypeInfo>");
            buf.append(obj->m_ReturnBinTypeInfo->str());
            buf.append("</ReturnBinTypeInfo>");
        }

        return 0;
    }

    int serialize(DFUFileAccessRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_RequestBase)
        {
            buf.append("<RequestBase>");
            serialize(obj->m_RequestBase, buf);
            buf.append("</RequestBase>");
        }

        return 0;
    }

    int serialize(DFUPartLocation* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_LocationIndex)
        {
            buf.append("<LocationIndex>");
            buf.append(obj->m_LocationIndex->str());
            buf.append("</LocationIndex>");
        }

        if (obj->m_Host)
        {
            buf.append("<Host>");
            buf.append(obj->m_Host->str());
            buf.append("</Host>");
        }

        return 0;
    }

    int serialize(DFUFileCopy* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_CopyIndex)
        {
            buf.append("<CopyIndex>");
            buf.append(obj->m_CopyIndex->str());
            buf.append("</CopyIndex>");
        }

        if (obj->m_LocationIndex)
        {
            buf.append("<LocationIndex>");
            buf.append(obj->m_LocationIndex->str());
            buf.append("</LocationIndex>");
        }

        if (obj->m_Path)
        {
            buf.append("<Path>");
            buf.append(obj->m_Path->str());
            buf.append("</Path>");
        }

        return 0;
    }

    int serialize(DFUFilePart* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_PartIndex)
        {
            buf.append("<PartIndex>");
            buf.append(obj->m_PartIndex->str());
            buf.append("</PartIndex>");
        }

        if (obj->m_Copies.length() > 0)
        {
            buf.append("<Copies>");
            ForEachItemIn(i, obj->m_Copies)
            {
                    DFUFileCopy& oneitem = obj->m_Copies.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</Copies>");
        }

        if (obj->m_TopLevelKey)
        {
            buf.append("<TopLevelKey>");
            buf.append(obj->m_TopLevelKey->str());
            buf.append("</TopLevelKey>");
        }

        return 0;
    }

    int serialize(DFUFileAccessInfo* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_MetaInfoBlob)
        {
            buf.append("<MetaInfoBlob>");
            buf.append(obj->m_MetaInfoBlob->str());
            buf.append("</MetaInfoBlob>");
        }

        if (obj->m_ExpiryTime)
        {
            buf.append("<ExpiryTime>");
            buf.append(obj->m_ExpiryTime->str());
            buf.append("</ExpiryTime>");
        }

        if (obj->m_NumParts)
        {
            buf.append("<NumParts>");
            buf.append(obj->m_NumParts->str());
            buf.append("</NumParts>");
        }

        if (obj->m_FileLocations.length() > 0)
        {
            buf.append("<FileLocations>");
            ForEachItemIn(i, obj->m_FileLocations)
            {
                    DFUPartLocation& oneitem = obj->m_FileLocations.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</FileLocations>");
        }

        if (obj->m_FileParts.length() > 0)
        {
            buf.append("<FileParts>");
            ForEachItemIn(i, obj->m_FileParts)
            {
                    DFUFilePart& oneitem = obj->m_FileParts.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</FileParts>");
        }

        if (obj->m_RecordTypeInfoBin)
        {
            buf.append("<RecordTypeInfoBin>");
            buf.append(obj->m_RecordTypeInfoBin->str());
            buf.append("</RecordTypeInfoBin>");
        }

        if (obj->m_RecordTypeInfoJson)
        {
            buf.append("<RecordTypeInfoJson>");
            buf.append(obj->m_RecordTypeInfoJson->str());
            buf.append("</RecordTypeInfoJson>");
        }

        if (obj->m_fileAccessPort)
        {
            buf.append("<fileAccessPort>");
            buf.append(obj->m_fileAccessPort->str());
            buf.append("</fileAccessPort>");
        }

        if (obj->m_fileAccessSSL)
        {
            buf.append("<fileAccessSSL>");
            buf.append(obj->m_fileAccessSSL->str());
            buf.append("</fileAccessSSL>");
        }

        return 0;
    }

    int serialize(DFUFileAccessResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_AccessInfo)
        {
            buf.append("<AccessInfo>");
            serialize(obj->m_AccessInfo, buf);
            buf.append("</AccessInfo>");
        }

        if (obj->m_Type != DFUFileType::UNSET)
            buf.append("<Type>").append(EnumHandlerDFUFileType::toString(obj->m_Type)).append("</Type>");

        return 0;
    }

    int serialize(DFUFileAccessV2Request* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_RequestId)
        {
            buf.append("<RequestId>");
            buf.append(obj->m_RequestId->str());
            buf.append("</RequestId>");
        }

        if (obj->m_ExpirySeconds)
        {
            buf.append("<ExpirySeconds>");
            buf.append(obj->m_ExpirySeconds->str());
            buf.append("</ExpirySeconds>");
        }

        if (obj->m_ReturnTextResponse)
        {
            buf.append("<ReturnTextResponse>");
            buf.append(obj->m_ReturnTextResponse->str());
            buf.append("</ReturnTextResponse>");
        }

        if (obj->m_SessionId)
        {
            buf.append("<SessionId>");
            buf.append(obj->m_SessionId->str());
            buf.append("</SessionId>");
        }

        if (obj->m_LockTimeoutMs)
        {
            buf.append("<LockTimeoutMs>");
            buf.append(obj->m_LockTimeoutMs->str());
            buf.append("</LockTimeoutMs>");
        }

        return 0;
    }

    int serialize(DFUFileCreateRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ECLRecordDefinition)
        {
            buf.append("<ECLRecordDefinition>");
            buf.append(obj->m_ECLRecordDefinition->str());
            buf.append("</ECLRecordDefinition>");
        }

        if (obj->m_PartLocations.length() > 0)
        {
            buf.append("<PartLocations>");
            ForEachItemIn(i, obj->m_PartLocations)
            {
            PString& oneitem = obj->m_PartLocations.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</PartLocations>");
        }

        if (obj->m_RequestBase)
        {
            buf.append("<RequestBase>");
            serialize(obj->m_RequestBase, buf);
            buf.append("</RequestBase>");
        }

        return 0;
    }

    int serialize(DFUFileCreateResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FileId)
        {
            buf.append("<FileId>");
            buf.append(obj->m_FileId->str());
            buf.append("</FileId>");
        }

        if (obj->m_Warning)
        {
            buf.append("<Warning>");
            buf.append(obj->m_Warning->str());
            buf.append("</Warning>");
        }

        if (obj->m_AccessInfo)
        {
            buf.append("<AccessInfo>");
            serialize(obj->m_AccessInfo, buf);
            buf.append("</AccessInfo>");
        }

        return 0;
    }

    int serialize(DFUFileCreateV2Request* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_Type != DFUFileType::UNSET)
            buf.append("<Type>").append(EnumHandlerDFUFileType::toString(obj->m_Type)).append("</Type>");

        if (obj->m_ECLRecordDefinition)
        {
            buf.append("<ECLRecordDefinition>");
            buf.append(obj->m_ECLRecordDefinition->str());
            buf.append("</ECLRecordDefinition>");
        }

        if (obj->m_RequestId)
        {
            buf.append("<RequestId>");
            buf.append(obj->m_RequestId->str());
            buf.append("</RequestId>");
        }

        if (obj->m_ExpirySeconds)
        {
            buf.append("<ExpirySeconds>");
            buf.append(obj->m_ExpirySeconds->str());
            buf.append("</ExpirySeconds>");
        }

        if (obj->m_ReturnTextResponse)
        {
            buf.append("<ReturnTextResponse>");
            buf.append(obj->m_ReturnTextResponse->str());
            buf.append("</ReturnTextResponse>");
        }

        if (obj->m_Compressed)
        {
            buf.append("<Compressed>");
            buf.append(obj->m_Compressed->str());
            buf.append("</Compressed>");
        }

        if (obj->m_SessionId)
        {
            buf.append("<SessionId>");
            buf.append(obj->m_SessionId->str());
            buf.append("</SessionId>");
        }

        if (obj->m_LockTimeoutMs)
        {
            buf.append("<LockTimeoutMs>");
            buf.append(obj->m_LockTimeoutMs->str());
            buf.append("</LockTimeoutMs>");
        }

        return 0;
    }

    int serialize(DFUFilePublishRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FileId)
        {
            buf.append("<FileId>");
            buf.append(obj->m_FileId->str());
            buf.append("</FileId>");
        }

        if (obj->m_Overwrite)
        {
            buf.append("<Overwrite>");
            buf.append(obj->m_Overwrite->str());
            buf.append("</Overwrite>");
        }

        if (obj->m_FileDescriptorBlob)
        {
            buf.append("<FileDescriptorBlob>");
            buf.append(obj->m_FileDescriptorBlob->str());
            buf.append("</FileDescriptorBlob>");
        }

        if (obj->m_SessionId)
        {
            buf.append("<SessionId>");
            buf.append(obj->m_SessionId->str());
            buf.append("</SessionId>");
        }

        if (obj->m_LockTimeoutMs)
        {
            buf.append("<LockTimeoutMs>");
            buf.append(obj->m_LockTimeoutMs->str());
            buf.append("</LockTimeoutMs>");
        }

        if (obj->m_ECLRecordDefinition)
        {
            buf.append("<ECLRecordDefinition>");
            buf.append(obj->m_ECLRecordDefinition->str());
            buf.append("</ECLRecordDefinition>");
        }

        if (obj->m_RecordCount)
        {
            buf.append("<RecordCount>");
            buf.append(obj->m_RecordCount->str());
            buf.append("</RecordCount>");
        }

        if (obj->m_FileSize)
        {
            buf.append("<FileSize>");
            buf.append(obj->m_FileSize->str());
            buf.append("</FileSize>");
        }

        return 0;
    }

    int serialize(DFUFilePublishResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(DFUFileViewRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Scope)
        {
            buf.append("<Scope>");
            buf.append(obj->m_Scope->str());
            buf.append("</Scope>");
        }

        if (obj->m_IncludeSuperOwner)
        {
            buf.append("<IncludeSuperOwner>");
            buf.append(obj->m_IncludeSuperOwner->str());
            buf.append("</IncludeSuperOwner>");
        }

        return 0;
    }

    int serialize(DFULogicalFile* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Prefix)
        {
            buf.append("<Prefix>");
            buf.append(obj->m_Prefix->str());
            buf.append("</Prefix>");
        }

        if (obj->m_ClusterName)
        {
            buf.append("<ClusterName>");
            buf.append(obj->m_ClusterName->str());
            buf.append("</ClusterName>");
        }

        if (obj->m_NodeGroup)
        {
            buf.append("<NodeGroup>");
            buf.append(obj->m_NodeGroup->str());
            buf.append("</NodeGroup>");
        }

        if (obj->m_Directory)
        {
            buf.append("<Directory>");
            buf.append(obj->m_Directory->str());
            buf.append("</Directory>");
        }

        if (obj->m_Description)
        {
            buf.append("<Description>");
            buf.append(obj->m_Description->str());
            buf.append("</Description>");
        }

        if (obj->m_Parts)
        {
            buf.append("<Parts>");
            buf.append(obj->m_Parts->str());
            buf.append("</Parts>");
        }

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_Totalsize)
        {
            buf.append("<Totalsize>");
            buf.append(obj->m_Totalsize->str());
            buf.append("</Totalsize>");
        }

        if (obj->m_RecordCount)
        {
            buf.append("<RecordCount>");
            buf.append(obj->m_RecordCount->str());
            buf.append("</RecordCount>");
        }

        if (obj->m_Modified)
        {
            buf.append("<Modified>");
            buf.append(obj->m_Modified->str());
            buf.append("</Modified>");
        }

        if (obj->m_LongSize)
        {
            buf.append("<LongSize>");
            buf.append(obj->m_LongSize->str());
            buf.append("</LongSize>");
        }

        if (obj->m_LongRecordCount)
        {
            buf.append("<LongRecordCount>");
            buf.append(obj->m_LongRecordCount->str());
            buf.append("</LongRecordCount>");
        }

        if (obj->m_isSuperfile)
        {
            buf.append("<isSuperfile>");
            buf.append(obj->m_isSuperfile->str());
            buf.append("</isSuperfile>");
        }

        if (obj->m_isZipfile)
        {
            buf.append("<isZipfile>");
            buf.append(obj->m_isZipfile->str());
            buf.append("</isZipfile>");
        }

        if (obj->m_isDirectory)
        {
            buf.append("<isDirectory>");
            buf.append(obj->m_isDirectory->str());
            buf.append("</isDirectory>");
        }

        if (obj->m_Replicate)
        {
            buf.append("<Replicate>");
            buf.append(obj->m_Replicate->str());
            buf.append("</Replicate>");
        }

        if (obj->m_IntSize)
        {
            buf.append("<IntSize>");
            buf.append(obj->m_IntSize->str());
            buf.append("</IntSize>");
        }

        if (obj->m_IntRecordCount)
        {
            buf.append("<IntRecordCount>");
            buf.append(obj->m_IntRecordCount->str());
            buf.append("</IntRecordCount>");
        }

        if (obj->m_FromRoxieCluster)
        {
            buf.append("<FromRoxieCluster>");
            buf.append(obj->m_FromRoxieCluster->str());
            buf.append("</FromRoxieCluster>");
        }

        if (obj->m_BrowseData)
        {
            buf.append("<BrowseData>");
            buf.append(obj->m_BrowseData->str());
            buf.append("</BrowseData>");
        }

        if (obj->m_IsKeyFile)
        {
            buf.append("<IsKeyFile>");
            buf.append(obj->m_IsKeyFile->str());
            buf.append("</IsKeyFile>");
        }

        if (obj->m_IsCompressed)
        {
            buf.append("<IsCompressed>");
            buf.append(obj->m_IsCompressed->str());
            buf.append("</IsCompressed>");
        }

        if (obj->m_ContentType)
        {
            buf.append("<ContentType>");
            buf.append(obj->m_ContentType->str());
            buf.append("</ContentType>");
        }

        if (obj->m_CompressedFileSize)
        {
            buf.append("<CompressedFileSize>");
            buf.append(obj->m_CompressedFileSize->str());
            buf.append("</CompressedFileSize>");
        }

        if (obj->m_SuperOwners)
        {
            buf.append("<SuperOwners>");
            buf.append(obj->m_SuperOwners->str());
            buf.append("</SuperOwners>");
        }

        if (obj->m_Persistent)
        {
            buf.append("<Persistent>");
            buf.append(obj->m_Persistent->str());
            buf.append("</Persistent>");
        }

        if (obj->m_IsProtected)
        {
            buf.append("<IsProtected>");
            buf.append(obj->m_IsProtected->str());
            buf.append("</IsProtected>");
        }

        if (obj->m_KeyType)
        {
            buf.append("<KeyType>");
            buf.append(obj->m_KeyType->str());
            buf.append("</KeyType>");
        }

        if (obj->m_NumOfSubfiles)
        {
            buf.append("<NumOfSubfiles>");
            buf.append(obj->m_NumOfSubfiles->str());
            buf.append("</NumOfSubfiles>");
        }

        if (obj->m_Accessed)
        {
            buf.append("<Accessed>");
            buf.append(obj->m_Accessed->str());
            buf.append("</Accessed>");
        }

        return 0;
    }

    int serialize(DFUFileViewResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Scope)
        {
            buf.append("<Scope>");
            buf.append(obj->m_Scope->str());
            buf.append("</Scope>");
        }

        if (obj->m_NumFiles)
        {
            buf.append("<NumFiles>");
            buf.append(obj->m_NumFiles->str());
            buf.append("</NumFiles>");
        }

        if (obj->m_DFULogicalFiles.length() > 0)
        {
            buf.append("<DFULogicalFiles>");
            ForEachItemIn(i, obj->m_DFULogicalFiles)
            {
                    DFULogicalFile& oneitem = obj->m_DFULogicalFiles.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFULogicalFiles>");
        }

        return 0;
    }

    int serialize(DFUGetDataColumnsRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_OpenLogicalName)
        {
            buf.append("<OpenLogicalName>");
            buf.append(obj->m_OpenLogicalName->str());
            buf.append("</OpenLogicalName>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_FilterBy)
        {
            buf.append("<FilterBy>");
            buf.append(obj->m_FilterBy->str());
            buf.append("</FilterBy>");
        }

        if (obj->m_ShowColumns)
        {
            buf.append("<ShowColumns>");
            buf.append(obj->m_ShowColumns->str());
            buf.append("</ShowColumns>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        if (obj->m_StartIndex)
        {
            buf.append("<StartIndex>");
            buf.append(obj->m_StartIndex->str());
            buf.append("</StartIndex>");
        }

        if (obj->m_EndIndex)
        {
            buf.append("<EndIndex>");
            buf.append(obj->m_EndIndex->str());
            buf.append("</EndIndex>");
        }

        return 0;
    }

    int serialize(DFUGetDataColumnsResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_StartIndex)
        {
            buf.append("<StartIndex>");
            buf.append(obj->m_StartIndex->str());
            buf.append("</StartIndex>");
        }

        if (obj->m_EndIndex)
        {
            buf.append("<EndIndex>");
            buf.append(obj->m_EndIndex->str());
            buf.append("</EndIndex>");
        }

        if (obj->m_DFUDataKeyedColumns1.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns1>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns1)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns1.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns1>");
        }

        if (obj->m_DFUDataKeyedColumns2.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns2>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns2)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns2.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns2>");
        }

        if (obj->m_DFUDataKeyedColumns3.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns3>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns3)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns3.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns3>");
        }

        if (obj->m_DFUDataKeyedColumns4.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns4>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns4)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns4.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns4>");
        }

        if (obj->m_DFUDataKeyedColumns5.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns5>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns5)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns5.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns5>");
        }

        if (obj->m_DFUDataKeyedColumns6.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns6>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns6)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns6.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns6>");
        }

        if (obj->m_DFUDataKeyedColumns7.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns7>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns7)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns7.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns7>");
        }

        if (obj->m_DFUDataKeyedColumns8.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns8>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns8)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns8.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns8>");
        }

        if (obj->m_DFUDataKeyedColumns9.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns9>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns9)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns9.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns9>");
        }

        if (obj->m_DFUDataKeyedColumns10.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns10>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns10)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns10.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns10>");
        }

        if (obj->m_DFUDataKeyedColumns11.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns11>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns11)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns11.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns11>");
        }

        if (obj->m_DFUDataKeyedColumns12.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns12>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns12)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns12.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns12>");
        }

        if (obj->m_DFUDataKeyedColumns13.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns13>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns13)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns13.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns13>");
        }

        if (obj->m_DFUDataKeyedColumns14.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns14>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns14)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns14.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns14>");
        }

        if (obj->m_DFUDataKeyedColumns15.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns15>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns15)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns15.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns15>");
        }

        if (obj->m_DFUDataKeyedColumns16.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns16>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns16)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns16.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns16>");
        }

        if (obj->m_DFUDataKeyedColumns17.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns17>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns17)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns17.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns17>");
        }

        if (obj->m_DFUDataKeyedColumns18.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns18>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns18)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns18.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns18>");
        }

        if (obj->m_DFUDataKeyedColumns19.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns19>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns19)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns19.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns19>");
        }

        if (obj->m_DFUDataKeyedColumns20.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns20>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns20)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns20.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns20>");
        }

        if (obj->m_DFUDataNonKeyedColumns1.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns1>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns1)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns1.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns1>");
        }

        if (obj->m_DFUDataNonKeyedColumns2.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns2>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns2)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns2.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns2>");
        }

        if (obj->m_DFUDataNonKeyedColumns3.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns3>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns3)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns3.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns3>");
        }

        if (obj->m_DFUDataNonKeyedColumns4.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns4>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns4)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns4.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns4>");
        }

        if (obj->m_DFUDataNonKeyedColumns5.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns5>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns5)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns5.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns5>");
        }

        if (obj->m_DFUDataNonKeyedColumns6.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns6>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns6)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns6.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns6>");
        }

        if (obj->m_DFUDataNonKeyedColumns7.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns7>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns7)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns7.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns7>");
        }

        if (obj->m_DFUDataNonKeyedColumns8.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns8>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns8)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns8.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns8>");
        }

        if (obj->m_DFUDataNonKeyedColumns9.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns9>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns9)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns9.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns9>");
        }

        if (obj->m_DFUDataNonKeyedColumns10.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns10>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns10)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns10.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns10>");
        }

        if (obj->m_DFUDataNonKeyedColumns11.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns11>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns11)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns11.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns11>");
        }

        if (obj->m_DFUDataNonKeyedColumns12.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns12>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns12)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns12.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns12>");
        }

        if (obj->m_DFUDataNonKeyedColumns13.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns13>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns13)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns13.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns13>");
        }

        if (obj->m_DFUDataNonKeyedColumns14.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns14>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns14)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns14.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns14>");
        }

        if (obj->m_DFUDataNonKeyedColumns15.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns15>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns15)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns15.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns15>");
        }

        if (obj->m_DFUDataNonKeyedColumns16.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns16>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns16)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns16.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns16>");
        }

        if (obj->m_DFUDataNonKeyedColumns17.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns17>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns17)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns17.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns17>");
        }

        if (obj->m_DFUDataNonKeyedColumns18.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns18>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns18)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns18.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns18>");
        }

        if (obj->m_DFUDataNonKeyedColumns19.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns19>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns19)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns19.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns19>");
        }

        if (obj->m_DFUDataNonKeyedColumns20.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns20>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns20)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns20.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns20>");
        }

        if (obj->m_RowCount)
        {
            buf.append("<RowCount>");
            buf.append(obj->m_RowCount->str());
            buf.append("</RowCount>");
        }

        if (obj->m_ShowColumns)
        {
            buf.append("<ShowColumns>");
            buf.append(obj->m_ShowColumns->str());
            buf.append("</ShowColumns>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        return 0;
    }

    int serialize(DFUGetFileMetaDataRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_LogicalFileName)
        {
            buf.append("<LogicalFileName>");
            buf.append(obj->m_LogicalFileName->str());
            buf.append("</LogicalFileName>");
        }

        if (obj->m_ClusterName)
        {
            buf.append("<ClusterName>");
            buf.append(obj->m_ClusterName->str());
            buf.append("</ClusterName>");
        }

        if (obj->m_IncludeXmlSchema)
        {
            buf.append("<IncludeXmlSchema>");
            buf.append(obj->m_IncludeXmlSchema->str());
            buf.append("</IncludeXmlSchema>");
        }

        if (obj->m_AddHeaderInXmlSchema)
        {
            buf.append("<AddHeaderInXmlSchema>");
            buf.append(obj->m_AddHeaderInXmlSchema->str());
            buf.append("</AddHeaderInXmlSchema>");
        }

        if (obj->m_IncludeXmlXPathSchema)
        {
            buf.append("<IncludeXmlXPathSchema>");
            buf.append(obj->m_IncludeXmlXPathSchema->str());
            buf.append("</IncludeXmlXPathSchema>");
        }

        if (obj->m_AddHeaderInXmlXPathSchema)
        {
            buf.append("<AddHeaderInXmlXPathSchema>");
            buf.append(obj->m_AddHeaderInXmlXPathSchema->str());
            buf.append("</AddHeaderInXmlXPathSchema>");
        }

        return 0;
    }

    int serialize(DFUGetFileMetaDataResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_TotalColumnCount)
        {
            buf.append("<TotalColumnCount>");
            buf.append(obj->m_TotalColumnCount->str());
            buf.append("</TotalColumnCount>");
        }

        if (obj->m_KeyedColumnCount)
        {
            buf.append("<KeyedColumnCount>");
            buf.append(obj->m_KeyedColumnCount->str());
            buf.append("</KeyedColumnCount>");
        }

        if (obj->m_DataColumns.length() > 0)
        {
            buf.append("<DataColumns>");
            ForEachItemIn(i, obj->m_DataColumns)
            {
                    DFUDataColumn& oneitem = obj->m_DataColumns.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DataColumns>");
        }

        if (obj->m_XmlSchema)
        {
            buf.append("<XmlSchema>");
            buf.append(obj->m_XmlSchema->str());
            buf.append("</XmlSchema>");
        }

        if (obj->m_XmlXPathSchema)
        {
            buf.append("<XmlXPathSchema>");
            buf.append(obj->m_XmlXPathSchema->str());
            buf.append("</XmlXPathSchema>");
        }

        if (obj->m_TotalResultRows)
        {
            buf.append("<TotalResultRows>");
            buf.append(obj->m_TotalResultRows->str());
            buf.append("</TotalResultRows>");
        }

        return 0;
    }

    int serialize(DFUInfoRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_UpdateDescription)
        {
            buf.append("<UpdateDescription>");
            buf.append(obj->m_UpdateDescription->str());
            buf.append("</UpdateDescription>");
        }

        if (obj->m_QuerySet)
        {
            buf.append("<QuerySet>");
            buf.append(obj->m_QuerySet->str());
            buf.append("</QuerySet>");
        }

        if (obj->m_Query)
        {
            buf.append("<Query>");
            buf.append(obj->m_Query->str());
            buf.append("</Query>");
        }

        if (obj->m_FileName)
        {
            buf.append("<FileName>");
            buf.append(obj->m_FileName->str());
            buf.append("</FileName>");
        }

        if (obj->m_FileDesc)
        {
            buf.append("<FileDesc>");
            buf.append(obj->m_FileDesc->str());
            buf.append("</FileDesc>");
        }

        if (obj->m_IncludeJsonTypeInfo)
        {
            buf.append("<IncludeJsonTypeInfo>");
            buf.append(obj->m_IncludeJsonTypeInfo->str());
            buf.append("</IncludeJsonTypeInfo>");
        }

        if (obj->m_IncludeBinTypeInfo)
        {
            buf.append("<IncludeBinTypeInfo>");
            buf.append(obj->m_IncludeBinTypeInfo->str());
            buf.append("</IncludeBinTypeInfo>");
        }

        if (obj->m_Protect != DFUChangeProtection::UNSET)
            buf.append("<Protect>").append(EnumHandlerDFUChangeProtection::toString(obj->m_Protect)).append("</Protect>");

        if (obj->m_Restrict != DFUChangeRestriction::UNSET)
            buf.append("<Restrict>").append(EnumHandlerDFUChangeRestriction::toString(obj->m_Restrict)).append("</Restrict>");

        return 0;
    }

    int serialize(DFUFileStat* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_MinSkew)
        {
            buf.append("<MinSkew>");
            buf.append(obj->m_MinSkew->str());
            buf.append("</MinSkew>");
        }

        if (obj->m_MaxSkew)
        {
            buf.append("<MaxSkew>");
            buf.append(obj->m_MaxSkew->str());
            buf.append("</MaxSkew>");
        }

        if (obj->m_MinSkewInt64)
        {
            buf.append("<MinSkewInt64>");
            buf.append(obj->m_MinSkewInt64->str());
            buf.append("</MinSkewInt64>");
        }

        if (obj->m_MaxSkewInt64)
        {
            buf.append("<MaxSkewInt64>");
            buf.append(obj->m_MaxSkewInt64->str());
            buf.append("</MaxSkewInt64>");
        }

        if (obj->m_MinSkewPart)
        {
            buf.append("<MinSkewPart>");
            buf.append(obj->m_MinSkewPart->str());
            buf.append("</MinSkewPart>");
        }

        if (obj->m_MaxSkewPart)
        {
            buf.append("<MaxSkewPart>");
            buf.append(obj->m_MaxSkewPart->str());
            buf.append("</MaxSkewPart>");
        }

        return 0;
    }

    int serialize(DFUPart* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Id)
        {
            buf.append("<Id>");
            buf.append(obj->m_Id->str());
            buf.append("</Id>");
        }

        if (obj->m_Copy)
        {
            buf.append("<Copy>");
            buf.append(obj->m_Copy->str());
            buf.append("</Copy>");
        }

        if (obj->m_ActualSize)
        {
            buf.append("<ActualSize>");
            buf.append(obj->m_ActualSize->str());
            buf.append("</ActualSize>");
        }

        if (obj->m_Ip)
        {
            buf.append("<Ip>");
            buf.append(obj->m_Ip->str());
            buf.append("</Ip>");
        }

        if (obj->m_Partsize)
        {
            buf.append("<Partsize>");
            buf.append(obj->m_Partsize->str());
            buf.append("</Partsize>");
        }

        if (obj->m_PartSizeInt64)
        {
            buf.append("<PartSizeInt64>");
            buf.append(obj->m_PartSizeInt64->str());
            buf.append("</PartSizeInt64>");
        }

        return 0;
    }

    int serialize(DFUFilePartsOnCluster* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_BaseDir)
        {
            buf.append("<BaseDir>");
            buf.append(obj->m_BaseDir->str());
            buf.append("</BaseDir>");
        }

        if (obj->m_ReplicateDir)
        {
            buf.append("<ReplicateDir>");
            buf.append(obj->m_ReplicateDir->str());
            buf.append("</ReplicateDir>");
        }

        if (obj->m_Replicate)
        {
            buf.append("<Replicate>");
            buf.append(obj->m_Replicate->str());
            buf.append("</Replicate>");
        }

        if (obj->m_CanReplicate)
        {
            buf.append("<CanReplicate>");
            buf.append(obj->m_CanReplicate->str());
            buf.append("</CanReplicate>");
        }

        if (obj->m_DFUFileParts.length() > 0)
        {
            buf.append("<DFUFileParts>");
            ForEachItemIn(i, obj->m_DFUFileParts)
            {
                    DFUPart& oneitem = obj->m_DFUFileParts.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUFileParts>");
        }

        return 0;
    }

    int serialize(DFUFileProtect* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_Count)
        {
            buf.append("<Count>");
            buf.append(obj->m_Count->str());
            buf.append("</Count>");
        }

        if (obj->m_Modified)
        {
            buf.append("<Modified>");
            buf.append(obj->m_Modified->str());
            buf.append("</Modified>");
        }

        return 0;
    }

    int serialize(DFUFilePartition* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FieldMask)
        {
            buf.append("<FieldMask>");
            buf.append(obj->m_FieldMask->str());
            buf.append("</FieldMask>");
        }

        if (obj->m_FieldNames.length() > 0)
        {
            buf.append("<FieldNames>");
            ForEachItemIn(i, obj->m_FieldNames)
            {
            PString& oneitem = obj->m_FieldNames.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</FieldNames>");
        }

        return 0;
    }

    int serialize(DFUFileBloom* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FieldMask)
        {
            buf.append("<FieldMask>");
            buf.append(obj->m_FieldMask->str());
            buf.append("</FieldMask>");
        }

        if (obj->m_FieldNames.length() > 0)
        {
            buf.append("<FieldNames>");
            ForEachItemIn(i, obj->m_FieldNames)
            {
            PString& oneitem = obj->m_FieldNames.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</FieldNames>");
        }

        if (obj->m_Limit)
        {
            buf.append("<Limit>");
            buf.append(obj->m_Limit->str());
            buf.append("</Limit>");
        }

        if (obj->m_Probability)
        {
            buf.append("<Probability>");
            buf.append(obj->m_Probability->str());
            buf.append("</Probability>");
        }

        return 0;
    }

    int serialize(DFUFileDetail* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Filename)
        {
            buf.append("<Filename>");
            buf.append(obj->m_Filename->str());
            buf.append("</Filename>");
        }

        if (obj->m_Prefix)
        {
            buf.append("<Prefix>");
            buf.append(obj->m_Prefix->str());
            buf.append("</Prefix>");
        }

        if (obj->m_NodeGroup)
        {
            buf.append("<NodeGroup>");
            buf.append(obj->m_NodeGroup->str());
            buf.append("</NodeGroup>");
        }

        if (obj->m_NumParts)
        {
            buf.append("<NumParts>");
            buf.append(obj->m_NumParts->str());
            buf.append("</NumParts>");
        }

        if (obj->m_Description)
        {
            buf.append("<Description>");
            buf.append(obj->m_Description->str());
            buf.append("</Description>");
        }

        if (obj->m_Dir)
        {
            buf.append("<Dir>");
            buf.append(obj->m_Dir->str());
            buf.append("</Dir>");
        }

        if (obj->m_PathMask)
        {
            buf.append("<PathMask>");
            buf.append(obj->m_PathMask->str());
            buf.append("</PathMask>");
        }

        if (obj->m_Filesize)
        {
            buf.append("<Filesize>");
            buf.append(obj->m_Filesize->str());
            buf.append("</Filesize>");
        }

        if (obj->m_FileSizeInt64)
        {
            buf.append("<FileSizeInt64>");
            buf.append(obj->m_FileSizeInt64->str());
            buf.append("</FileSizeInt64>");
        }

        if (obj->m_ActualSize)
        {
            buf.append("<ActualSize>");
            buf.append(obj->m_ActualSize->str());
            buf.append("</ActualSize>");
        }

        if (obj->m_RecordSize)
        {
            buf.append("<RecordSize>");
            buf.append(obj->m_RecordSize->str());
            buf.append("</RecordSize>");
        }

        if (obj->m_RecordCount)
        {
            buf.append("<RecordCount>");
            buf.append(obj->m_RecordCount->str());
            buf.append("</RecordCount>");
        }

        if (obj->m_RecordSizeInt64)
        {
            buf.append("<RecordSizeInt64>");
            buf.append(obj->m_RecordSizeInt64->str());
            buf.append("</RecordSizeInt64>");
        }

        if (obj->m_RecordCountInt64)
        {
            buf.append("<RecordCountInt64>");
            buf.append(obj->m_RecordCountInt64->str());
            buf.append("</RecordCountInt64>");
        }

        if (obj->m_Wuid)
        {
            buf.append("<Wuid>");
            buf.append(obj->m_Wuid->str());
            buf.append("</Wuid>");
        }

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_JobName)
        {
            buf.append("<JobName>");
            buf.append(obj->m_JobName->str());
            buf.append("</JobName>");
        }

        if (obj->m_Persistent)
        {
            buf.append("<Persistent>");
            buf.append(obj->m_Persistent->str());
            buf.append("</Persistent>");
        }

        if (obj->m_Format)
        {
            buf.append("<Format>");
            buf.append(obj->m_Format->str());
            buf.append("</Format>");
        }

        if (obj->m_MaxRecordSize)
        {
            buf.append("<MaxRecordSize>");
            buf.append(obj->m_MaxRecordSize->str());
            buf.append("</MaxRecordSize>");
        }

        if (obj->m_CsvSeparate)
        {
            buf.append("<CsvSeparate>");
            buf.append(obj->m_CsvSeparate->str());
            buf.append("</CsvSeparate>");
        }

        if (obj->m_CsvQuote)
        {
            buf.append("<CsvQuote>");
            buf.append(obj->m_CsvQuote->str());
            buf.append("</CsvQuote>");
        }

        if (obj->m_CsvTerminate)
        {
            buf.append("<CsvTerminate>");
            buf.append(obj->m_CsvTerminate->str());
            buf.append("</CsvTerminate>");
        }

        if (obj->m_CsvEscape)
        {
            buf.append("<CsvEscape>");
            buf.append(obj->m_CsvEscape->str());
            buf.append("</CsvEscape>");
        }

        if (obj->m_Modified)
        {
            buf.append("<Modified>");
            buf.append(obj->m_Modified->str());
            buf.append("</Modified>");
        }

        if (obj->m_Ecl)
        {
            buf.append("<Ecl>");
            buf.append(obj->m_Ecl->str());
            buf.append("</Ecl>");
        }

        if (obj->m_ZipFile)
        {
            buf.append("<ZipFile>");
            buf.append(obj->m_ZipFile->str());
            buf.append("</ZipFile>");
        }

        if (obj->m_Stat)
        {
            buf.append("<Stat>");
            serialize(obj->m_Stat, buf);
            buf.append("</Stat>");
        }

        if (obj->m_DFUFileParts.length() > 0)
        {
            buf.append("<DFUFileParts>");
            ForEachItemIn(i, obj->m_DFUFileParts)
            {
                    DFUPart& oneitem = obj->m_DFUFileParts.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUFileParts>");
        }

        if (obj->m_DFUFilePartsOnClusters.length() > 0)
        {
            buf.append("<DFUFilePartsOnClusters>");
            ForEachItemIn(i, obj->m_DFUFilePartsOnClusters)
            {
                    DFUFilePartsOnCluster& oneitem = obj->m_DFUFilePartsOnClusters.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUFilePartsOnClusters>");
        }

        if (obj->m_isSuperfile)
        {
            buf.append("<isSuperfile>");
            buf.append(obj->m_isSuperfile->str());
            buf.append("</isSuperfile>");
        }

        if (obj->m_ShowFileContent)
        {
            buf.append("<ShowFileContent>");
            buf.append(obj->m_ShowFileContent->str());
            buf.append("</ShowFileContent>");
        }

        if (obj->m_subfiles.length() > 0)
        {
            buf.append("<subfiles>");
            ForEachItemIn(i, obj->m_subfiles)
            {
            PString& oneitem = obj->m_subfiles.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</subfiles>");
        }

        if (obj->m_Superfiles.length() > 0)
        {
            buf.append("<Superfiles>");
            ForEachItemIn(i, obj->m_Superfiles)
            {
                    DFULogicalFile& oneitem = obj->m_Superfiles.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</Superfiles>");
        }

        if (obj->m_ProtectList.length() > 0)
        {
            buf.append("<ProtectList>");
            ForEachItemIn(i, obj->m_ProtectList)
            {
                    DFUFileProtect& oneitem = obj->m_ProtectList.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</ProtectList>");
        }

        if (obj->m_FromRoxieCluster)
        {
            buf.append("<FromRoxieCluster>");
            buf.append(obj->m_FromRoxieCluster->str());
            buf.append("</FromRoxieCluster>");
        }

        if (obj->m_Graphs.length() > 0)
        {
            buf.append("<Graphs>");
            ForEachItemIn(i, obj->m_Graphs)
            {
            PString& oneitem = obj->m_Graphs.item(i);
                buf.append("<ECLGraph>").append(oneitem.str()).append("</ECLGraph>");

            }
            buf.append("</Graphs>");
        }

        if (obj->m_UserPermission)
        {
            buf.append("<UserPermission>");
            buf.append(obj->m_UserPermission->str());
            buf.append("</UserPermission>");
        }

        if (obj->m_ContentType)
        {
            buf.append("<ContentType>");
            buf.append(obj->m_ContentType->str());
            buf.append("</ContentType>");
        }

        if (obj->m_CompressedFileSize)
        {
            buf.append("<CompressedFileSize>");
            buf.append(obj->m_CompressedFileSize->str());
            buf.append("</CompressedFileSize>");
        }

        if (obj->m_PercentCompressed)
        {
            buf.append("<PercentCompressed>");
            buf.append(obj->m_PercentCompressed->str());
            buf.append("</PercentCompressed>");
        }

        if (obj->m_IsCompressed)
        {
            buf.append("<IsCompressed>");
            buf.append(obj->m_IsCompressed->str());
            buf.append("</IsCompressed>");
        }

        if (obj->m_IsRestricted)
        {
            buf.append("<IsRestricted>");
            buf.append(obj->m_IsRestricted->str());
            buf.append("</IsRestricted>");
        }

        if (obj->m_BrowseData)
        {
            buf.append("<BrowseData>");
            buf.append(obj->m_BrowseData->str());
            buf.append("</BrowseData>");
        }

        if (obj->m_jsonInfo)
        {
            buf.append("<jsonInfo>");
            buf.append(obj->m_jsonInfo->str());
            buf.append("</jsonInfo>");
        }

        if (obj->m_binInfo)
        {
            buf.append("<binInfo>");
            buf.append(obj->m_binInfo->str());
            buf.append("</binInfo>");
        }

        if (obj->m_PackageID)
        {
            buf.append("<PackageID>");
            buf.append(obj->m_PackageID->str());
            buf.append("</PackageID>");
        }

        if (obj->m_Partition)
        {
            buf.append("<Partition>");
            serialize(obj->m_Partition, buf);
            buf.append("</Partition>");
        }

        if (obj->m_Blooms.length() > 0)
        {
            buf.append("<Blooms>");
            ForEachItemIn(i, obj->m_Blooms)
            {
                    DFUFileBloom& oneitem = obj->m_Blooms.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</Blooms>");
        }

        if (obj->m_ExpireDays)
        {
            buf.append("<ExpireDays>");
            buf.append(obj->m_ExpireDays->str());
            buf.append("</ExpireDays>");
        }

        if (obj->m_KeyType)
        {
            buf.append("<KeyType>");
            buf.append(obj->m_KeyType->str());
            buf.append("</KeyType>");
        }

        return 0;
    }

    int serialize(DFUInfoResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_FileDetail)
        {
            buf.append("<FileDetail>");
            serialize(obj->m_FileDetail, buf);
            buf.append("</FileDetail>");
        }

        return 0;
    }

    int serialize(DFUQueryRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Prefix)
        {
            buf.append("<Prefix>");
            buf.append(obj->m_Prefix->str());
            buf.append("</Prefix>");
        }

        if (obj->m_ClusterName)
        {
            buf.append("<ClusterName>");
            buf.append(obj->m_ClusterName->str());
            buf.append("</ClusterName>");
        }

        if (obj->m_NodeGroup)
        {
            buf.append("<NodeGroup>");
            buf.append(obj->m_NodeGroup->str());
            buf.append("</NodeGroup>");
        }

        if (obj->m_ContentType)
        {
            buf.append("<ContentType>");
            buf.append(obj->m_ContentType->str());
            buf.append("</ContentType>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_StartDate)
        {
            buf.append("<StartDate>");
            buf.append(obj->m_StartDate->str());
            buf.append("</StartDate>");
        }

        if (obj->m_EndDate)
        {
            buf.append("<EndDate>");
            buf.append(obj->m_EndDate->str());
            buf.append("</EndDate>");
        }

        if (obj->m_FileType)
        {
            buf.append("<FileType>");
            buf.append(obj->m_FileType->str());
            buf.append("</FileType>");
        }

        if (obj->m_FileSizeFrom)
        {
            buf.append("<FileSizeFrom>");
            buf.append(obj->m_FileSizeFrom->str());
            buf.append("</FileSizeFrom>");
        }

        if (obj->m_FileSizeTo)
        {
            buf.append("<FileSizeTo>");
            buf.append(obj->m_FileSizeTo->str());
            buf.append("</FileSizeTo>");
        }

        if (obj->m_FirstN)
        {
            buf.append("<FirstN>");
            buf.append(obj->m_FirstN->str());
            buf.append("</FirstN>");
        }

        if (obj->m_FirstNType)
        {
            buf.append("<FirstNType>");
            buf.append(obj->m_FirstNType->str());
            buf.append("</FirstNType>");
        }

        if (obj->m_PageSize)
        {
            buf.append("<PageSize>");
            buf.append(obj->m_PageSize->str());
            buf.append("</PageSize>");
        }

        if (obj->m_PageStartFrom)
        {
            buf.append("<PageStartFrom>");
            buf.append(obj->m_PageStartFrom->str());
            buf.append("</PageStartFrom>");
        }

        if (obj->m_Sortby)
        {
            buf.append("<Sortby>");
            buf.append(obj->m_Sortby->str());
            buf.append("</Sortby>");
        }

        if (obj->m_Descending)
        {
            buf.append("<Descending>");
            buf.append(obj->m_Descending->str());
            buf.append("</Descending>");
        }

        if (obj->m_OneLevelDirFileReturn)
        {
            buf.append("<OneLevelDirFileReturn>");
            buf.append(obj->m_OneLevelDirFileReturn->str());
            buf.append("</OneLevelDirFileReturn>");
        }

        if (obj->m_CacheHint)
        {
            buf.append("<CacheHint>");
            buf.append(obj->m_CacheHint->str());
            buf.append("</CacheHint>");
        }

        if (obj->m_MaxNumberOfFiles)
        {
            buf.append("<MaxNumberOfFiles>");
            buf.append(obj->m_MaxNumberOfFiles->str());
            buf.append("</MaxNumberOfFiles>");
        }

        if (obj->m_IncludeSuperOwner)
        {
            buf.append("<IncludeSuperOwner>");
            buf.append(obj->m_IncludeSuperOwner->str());
            buf.append("</IncludeSuperOwner>");
        }

        if (obj->m_StartAccessedTime)
        {
            buf.append("<StartAccessedTime>");
            buf.append(obj->m_StartAccessedTime->str());
            buf.append("</StartAccessedTime>");
        }

        if (obj->m_EndAccessedTime)
        {
            buf.append("<EndAccessedTime>");
            buf.append(obj->m_EndAccessedTime->str());
            buf.append("</EndAccessedTime>");
        }

        return 0;
    }

    int serialize(DFUQueryResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_DFULogicalFiles.length() > 0)
        {
            buf.append("<DFULogicalFiles>");
            ForEachItemIn(i, obj->m_DFULogicalFiles)
            {
                    DFULogicalFile& oneitem = obj->m_DFULogicalFiles.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFULogicalFiles>");
        }

        if (obj->m_Prefix)
        {
            buf.append("<Prefix>");
            buf.append(obj->m_Prefix->str());
            buf.append("</Prefix>");
        }

        if (obj->m_ClusterName)
        {
            buf.append("<ClusterName>");
            buf.append(obj->m_ClusterName->str());
            buf.append("</ClusterName>");
        }

        if (obj->m_NodeGroup)
        {
            buf.append("<NodeGroup>");
            buf.append(obj->m_NodeGroup->str());
            buf.append("</NodeGroup>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_Description)
        {
            buf.append("<Description>");
            buf.append(obj->m_Description->str());
            buf.append("</Description>");
        }

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_StartDate)
        {
            buf.append("<StartDate>");
            buf.append(obj->m_StartDate->str());
            buf.append("</StartDate>");
        }

        if (obj->m_EndDate)
        {
            buf.append("<EndDate>");
            buf.append(obj->m_EndDate->str());
            buf.append("</EndDate>");
        }

        if (obj->m_FileType)
        {
            buf.append("<FileType>");
            buf.append(obj->m_FileType->str());
            buf.append("</FileType>");
        }

        if (obj->m_FileSizeFrom)
        {
            buf.append("<FileSizeFrom>");
            buf.append(obj->m_FileSizeFrom->str());
            buf.append("</FileSizeFrom>");
        }

        if (obj->m_FileSizeTo)
        {
            buf.append("<FileSizeTo>");
            buf.append(obj->m_FileSizeTo->str());
            buf.append("</FileSizeTo>");
        }

        if (obj->m_FirstN)
        {
            buf.append("<FirstN>");
            buf.append(obj->m_FirstN->str());
            buf.append("</FirstN>");
        }

        if (obj->m_FirstNType)
        {
            buf.append("<FirstNType>");
            buf.append(obj->m_FirstNType->str());
            buf.append("</FirstNType>");
        }

        if (obj->m_PageSize)
        {
            buf.append("<PageSize>");
            buf.append(obj->m_PageSize->str());
            buf.append("</PageSize>");
        }

        if (obj->m_PageStartFrom)
        {
            buf.append("<PageStartFrom>");
            buf.append(obj->m_PageStartFrom->str());
            buf.append("</PageStartFrom>");
        }

        if (obj->m_LastPageFrom)
        {
            buf.append("<LastPageFrom>");
            buf.append(obj->m_LastPageFrom->str());
            buf.append("</LastPageFrom>");
        }

        if (obj->m_PageEndAt)
        {
            buf.append("<PageEndAt>");
            buf.append(obj->m_PageEndAt->str());
            buf.append("</PageEndAt>");
        }

        if (obj->m_PrevPageFrom)
        {
            buf.append("<PrevPageFrom>");
            buf.append(obj->m_PrevPageFrom->str());
            buf.append("</PrevPageFrom>");
        }

        if (obj->m_NextPageFrom)
        {
            buf.append("<NextPageFrom>");
            buf.append(obj->m_NextPageFrom->str());
            buf.append("</NextPageFrom>");
        }

        if (obj->m_NumFiles)
        {
            buf.append("<NumFiles>");
            buf.append(obj->m_NumFiles->str());
            buf.append("</NumFiles>");
        }

        if (obj->m_Sortby)
        {
            buf.append("<Sortby>");
            buf.append(obj->m_Sortby->str());
            buf.append("</Sortby>");
        }

        if (obj->m_Descending)
        {
            buf.append("<Descending>");
            buf.append(obj->m_Descending->str());
            buf.append("</Descending>");
        }

        if (obj->m_BasicQuery)
        {
            buf.append("<BasicQuery>");
            buf.append(obj->m_BasicQuery->str());
            buf.append("</BasicQuery>");
        }

        if (obj->m_ParametersForPaging)
        {
            buf.append("<ParametersForPaging>");
            buf.append(obj->m_ParametersForPaging->str());
            buf.append("</ParametersForPaging>");
        }

        if (obj->m_Filters)
        {
            buf.append("<Filters>");
            buf.append(obj->m_Filters->str());
            buf.append("</Filters>");
        }

        if (obj->m_CacheHint)
        {
            buf.append("<CacheHint>");
            buf.append(obj->m_CacheHint->str());
            buf.append("</CacheHint>");
        }

        if (obj->m_IsSubsetOfFiles)
        {
            buf.append("<IsSubsetOfFiles>");
            buf.append(obj->m_IsSubsetOfFiles->str());
            buf.append("</IsSubsetOfFiles>");
        }

        if (obj->m_Warning)
        {
            buf.append("<Warning>");
            buf.append(obj->m_Warning->str());
            buf.append("</Warning>");
        }

        return 0;
    }

    int serialize(DFURecordTypeInfoRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_IncludeJsonTypeInfo)
        {
            buf.append("<IncludeJsonTypeInfo>");
            buf.append(obj->m_IncludeJsonTypeInfo->str());
            buf.append("</IncludeJsonTypeInfo>");
        }

        if (obj->m_IncludeBinTypeInfo)
        {
            buf.append("<IncludeBinTypeInfo>");
            buf.append(obj->m_IncludeBinTypeInfo->str());
            buf.append("</IncludeBinTypeInfo>");
        }

        return 0;
    }

    int serialize(DFURecordTypeInfoResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_jsonInfo)
        {
            buf.append("<jsonInfo>");
            buf.append(obj->m_jsonInfo->str());
            buf.append("</jsonInfo>");
        }

        if (obj->m_binInfo)
        {
            buf.append("<binInfo>");
            buf.append(obj->m_binInfo->str());
            buf.append("</binInfo>");
        }

        return 0;
    }

    int serialize(DFUSearchRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ShowExample)
        {
            buf.append("<ShowExample>");
            buf.append(obj->m_ShowExample->str());
            buf.append("</ShowExample>");
        }

        return 0;
    }

    int serialize(DFUSearchResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_ShowExample)
        {
            buf.append("<ShowExample>");
            buf.append(obj->m_ShowExample->str());
            buf.append("</ShowExample>");
        }

        if (obj->m_ClusterNames.length() > 0)
        {
            buf.append("<ClusterNames>");
            ForEachItemIn(i, obj->m_ClusterNames)
            {
            PString& oneitem = obj->m_ClusterNames.item(i);
                buf.append("<ClusterName>").append(oneitem.str()).append("</ClusterName>");

            }
            buf.append("</ClusterNames>");
        }

        if (obj->m_FileTypes.length() > 0)
        {
            buf.append("<FileTypes>");
            ForEachItemIn(i, obj->m_FileTypes)
            {
            PString& oneitem = obj->m_FileTypes.item(i);
                buf.append("<FileType>").append(oneitem.str()).append("</FileType>");

            }
            buf.append("</FileTypes>");
        }

        return 0;
    }

    int serialize(DFUSearchDataRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        if (obj->m_OpenLogicalName)
        {
            buf.append("<OpenLogicalName>");
            buf.append(obj->m_OpenLogicalName->str());
            buf.append("</OpenLogicalName>");
        }

        if (obj->m_FilterBy)
        {
            buf.append("<FilterBy>");
            buf.append(obj->m_FilterBy->str());
            buf.append("</FilterBy>");
        }

        if (obj->m_ShowColumns)
        {
            buf.append("<ShowColumns>");
            buf.append(obj->m_ShowColumns->str());
            buf.append("</ShowColumns>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_StartIndex)
        {
            buf.append("<StartIndex>");
            buf.append(obj->m_StartIndex->str());
            buf.append("</StartIndex>");
        }

        if (obj->m_EndIndex)
        {
            buf.append("<EndIndex>");
            buf.append(obj->m_EndIndex->str());
            buf.append("</EndIndex>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_ParentName)
        {
            buf.append("<ParentName>");
            buf.append(obj->m_ParentName->str());
            buf.append("</ParentName>");
        }

        if (obj->m_StartForGoback)
        {
            buf.append("<StartForGoback>");
            buf.append(obj->m_StartForGoback->str());
            buf.append("</StartForGoback>");
        }

        if (obj->m_CountForGoback)
        {
            buf.append("<CountForGoback>");
            buf.append(obj->m_CountForGoback->str());
            buf.append("</CountForGoback>");
        }

        if (obj->m_Start)
        {
            buf.append("<Start>");
            buf.append(obj->m_Start->str());
            buf.append("</Start>");
        }

        if (obj->m_Count)
        {
            buf.append("<Count>");
            buf.append(obj->m_Count->str());
            buf.append("</Count>");
        }

        if (obj->m_File)
        {
            buf.append("<File>");
            buf.append(obj->m_File->str());
            buf.append("</File>");
        }

        if (obj->m_Key)
        {
            buf.append("<Key>");
            buf.append(obj->m_Key->str());
            buf.append("</Key>");
        }

        if (obj->m_SchemaOnly)
        {
            buf.append("<SchemaOnly>");
            buf.append(obj->m_SchemaOnly->str());
            buf.append("</SchemaOnly>");
        }

        if (obj->m_RoxieSelections)
        {
            buf.append("<RoxieSelections>");
            buf.append(obj->m_RoxieSelections->str());
            buf.append("</RoxieSelections>");
        }

        if (obj->m_DisableUppercaseTranslation)
        {
            buf.append("<DisableUppercaseTranslation>");
            buf.append(obj->m_DisableUppercaseTranslation->str());
            buf.append("</DisableUppercaseTranslation>");
        }

        if (obj->m_SelectedKey)
        {
            buf.append("<SelectedKey>");
            buf.append(obj->m_SelectedKey->str());
            buf.append("</SelectedKey>");
        }

        return 0;
    }

    int serialize(DFUSearchDataResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_OpenLogicalName)
        {
            buf.append("<OpenLogicalName>");
            buf.append(obj->m_OpenLogicalName->str());
            buf.append("</OpenLogicalName>");
        }

        if (obj->m_LogicalName)
        {
            buf.append("<LogicalName>");
            buf.append(obj->m_LogicalName->str());
            buf.append("</LogicalName>");
        }

        if (obj->m_ParentName)
        {
            buf.append("<ParentName>");
            buf.append(obj->m_ParentName->str());
            buf.append("</ParentName>");
        }

        if (obj->m_StartIndex)
        {
            buf.append("<StartIndex>");
            buf.append(obj->m_StartIndex->str());
            buf.append("</StartIndex>");
        }

        if (obj->m_EndIndex)
        {
            buf.append("<EndIndex>");
            buf.append(obj->m_EndIndex->str());
            buf.append("</EndIndex>");
        }

        if (obj->m_DFUDataKeyedColumns1.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns1>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns1)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns1.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns1>");
        }

        if (obj->m_DFUDataKeyedColumns2.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns2>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns2)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns2.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns2>");
        }

        if (obj->m_DFUDataKeyedColumns3.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns3>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns3)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns3.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns3>");
        }

        if (obj->m_DFUDataKeyedColumns4.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns4>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns4)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns4.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns4>");
        }

        if (obj->m_DFUDataKeyedColumns5.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns5>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns5)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns5.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns5>");
        }

        if (obj->m_DFUDataKeyedColumns6.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns6>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns6)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns6.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns6>");
        }

        if (obj->m_DFUDataKeyedColumns7.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns7>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns7)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns7.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns7>");
        }

        if (obj->m_DFUDataKeyedColumns8.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns8>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns8)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns8.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns8>");
        }

        if (obj->m_DFUDataKeyedColumns9.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns9>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns9)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns9.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns9>");
        }

        if (obj->m_DFUDataKeyedColumns10.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns10>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns10)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns10.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns10>");
        }

        if (obj->m_DFUDataKeyedColumns11.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns11>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns11)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns11.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns11>");
        }

        if (obj->m_DFUDataKeyedColumns12.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns12>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns12)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns12.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns12>");
        }

        if (obj->m_DFUDataKeyedColumns13.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns13>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns13)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns13.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns13>");
        }

        if (obj->m_DFUDataKeyedColumns14.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns14>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns14)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns14.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns14>");
        }

        if (obj->m_DFUDataKeyedColumns15.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns15>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns15)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns15.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns15>");
        }

        if (obj->m_DFUDataKeyedColumns16.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns16>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns16)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns16.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns16>");
        }

        if (obj->m_DFUDataKeyedColumns17.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns17>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns17)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns17.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns17>");
        }

        if (obj->m_DFUDataKeyedColumns18.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns18>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns18)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns18.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns18>");
        }

        if (obj->m_DFUDataKeyedColumns19.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns19>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns19)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns19.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns19>");
        }

        if (obj->m_DFUDataKeyedColumns20.length() > 0)
        {
            buf.append("<DFUDataKeyedColumns20>");
            ForEachItemIn(i, obj->m_DFUDataKeyedColumns20)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataKeyedColumns20.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataKeyedColumns20>");
        }

        if (obj->m_DFUDataNonKeyedColumns1.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns1>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns1)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns1.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns1>");
        }

        if (obj->m_DFUDataNonKeyedColumns2.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns2>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns2)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns2.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns2>");
        }

        if (obj->m_DFUDataNonKeyedColumns3.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns3>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns3)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns3.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns3>");
        }

        if (obj->m_DFUDataNonKeyedColumns4.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns4>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns4)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns4.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns4>");
        }

        if (obj->m_DFUDataNonKeyedColumns5.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns5>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns5)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns5.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns5>");
        }

        if (obj->m_DFUDataNonKeyedColumns6.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns6>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns6)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns6.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns6>");
        }

        if (obj->m_DFUDataNonKeyedColumns7.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns7>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns7)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns7.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns7>");
        }

        if (obj->m_DFUDataNonKeyedColumns8.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns8>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns8)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns8.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns8>");
        }

        if (obj->m_DFUDataNonKeyedColumns9.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns9>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns9)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns9.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns9>");
        }

        if (obj->m_DFUDataNonKeyedColumns10.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns10>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns10)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns10.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns10>");
        }

        if (obj->m_DFUDataNonKeyedColumns11.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns11>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns11)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns11.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns11>");
        }

        if (obj->m_DFUDataNonKeyedColumns12.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns12>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns12)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns12.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns12>");
        }

        if (obj->m_DFUDataNonKeyedColumns13.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns13>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns13)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns13.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns13>");
        }

        if (obj->m_DFUDataNonKeyedColumns14.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns14>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns14)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns14.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns14>");
        }

        if (obj->m_DFUDataNonKeyedColumns15.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns15>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns15)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns15.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns15>");
        }

        if (obj->m_DFUDataNonKeyedColumns16.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns16>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns16)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns16.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns16>");
        }

        if (obj->m_DFUDataNonKeyedColumns17.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns17>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns17)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns17.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns17>");
        }

        if (obj->m_DFUDataNonKeyedColumns18.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns18>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns18)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns18.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns18>");
        }

        if (obj->m_DFUDataNonKeyedColumns19.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns19>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns19)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns19.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns19>");
        }

        if (obj->m_DFUDataNonKeyedColumns20.length() > 0)
        {
            buf.append("<DFUDataNonKeyedColumns20>");
            ForEachItemIn(i, obj->m_DFUDataNonKeyedColumns20)
            {
                    DFUDataColumn& oneitem = obj->m_DFUDataNonKeyedColumns20.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUDataNonKeyedColumns20>");
        }

        if (obj->m_RowCount)
        {
            buf.append("<RowCount>");
            buf.append(obj->m_RowCount->str());
            buf.append("</RowCount>");
        }

        if (obj->m_ShowColumns)
        {
            buf.append("<ShowColumns>");
            buf.append(obj->m_ShowColumns->str());
            buf.append("</ShowColumns>");
        }

        if (obj->m_ChooseFile)
        {
            buf.append("<ChooseFile>");
            buf.append(obj->m_ChooseFile->str());
            buf.append("</ChooseFile>");
        }

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_FilterBy)
        {
            buf.append("<FilterBy>");
            buf.append(obj->m_FilterBy->str());
            buf.append("</FilterBy>");
        }

        if (obj->m_FilterForGoBack)
        {
            buf.append("<FilterForGoBack>");
            buf.append(obj->m_FilterForGoBack->str());
            buf.append("</FilterForGoBack>");
        }

        if (obj->m_ColumnsHidden.length() > 0)
        {
            buf.append("<ColumnsHidden>");
            ForEachItemIn(i, obj->m_ColumnsHidden)
            {
                    DFUDataColumn& oneitem = obj->m_ColumnsHidden.item(i);
                buf.append("<ColumnHidden>");
                serialize(&oneitem, buf);
                buf.append("</ColumnHidden>");

            }
            buf.append("</ColumnsHidden>");
        }

        if (obj->m_ColumnCount)
        {
            buf.append("<ColumnCount>");
            buf.append(obj->m_ColumnCount->str());
            buf.append("</ColumnCount>");
        }

        if (obj->m_StartForGoback)
        {
            buf.append("<StartForGoback>");
            buf.append(obj->m_StartForGoback->str());
            buf.append("</StartForGoback>");
        }

        if (obj->m_CountForGoback)
        {
            buf.append("<CountForGoback>");
            buf.append(obj->m_CountForGoback->str());
            buf.append("</CountForGoback>");
        }

        if (obj->m_Start)
        {
            buf.append("<Start>");
            buf.append(obj->m_Start->str());
            buf.append("</Start>");
        }

        if (obj->m_Count)
        {
            buf.append("<Count>");
            buf.append(obj->m_Count->str());
            buf.append("</Count>");
        }

        if (obj->m_PageSize)
        {
            buf.append("<PageSize>");
            buf.append(obj->m_PageSize->str());
            buf.append("</PageSize>");
        }

        if (obj->m_Total)
        {
            buf.append("<Total>");
            buf.append(obj->m_Total->str());
            buf.append("</Total>");
        }

        if (obj->m_Result)
        {
            buf.append("<Result>");
            buf.append(obj->m_Result->str());
            buf.append("</Result>");
        }

        if (obj->m_MsgToDisplay)
        {
            buf.append("<MsgToDisplay>");
            buf.append(obj->m_MsgToDisplay->str());
            buf.append("</MsgToDisplay>");
        }

        if (obj->m_Cluster)
        {
            buf.append("<Cluster>");
            buf.append(obj->m_Cluster->str());
            buf.append("</Cluster>");
        }

        if (obj->m_ClusterType)
        {
            buf.append("<ClusterType>");
            buf.append(obj->m_ClusterType->str());
            buf.append("</ClusterType>");
        }

        if (obj->m_File)
        {
            buf.append("<File>");
            buf.append(obj->m_File->str());
            buf.append("</File>");
        }

        if (obj->m_Key)
        {
            buf.append("<Key>");
            buf.append(obj->m_Key->str());
            buf.append("</Key>");
        }

        if (obj->m_SchemaOnly)
        {
            buf.append("<SchemaOnly>");
            buf.append(obj->m_SchemaOnly->str());
            buf.append("</SchemaOnly>");
        }

        if (obj->m_RoxieSelections)
        {
            buf.append("<RoxieSelections>");
            buf.append(obj->m_RoxieSelections->str());
            buf.append("</RoxieSelections>");
        }

        if (obj->m_DisableUppercaseTranslation)
        {
            buf.append("<DisableUppercaseTranslation>");
            buf.append(obj->m_DisableUppercaseTranslation->str());
            buf.append("</DisableUppercaseTranslation>");
        }

        if (obj->m_AutoUppercaseTranslation)
        {
            buf.append("<AutoUppercaseTranslation>");
            buf.append(obj->m_AutoUppercaseTranslation->str());
            buf.append("</AutoUppercaseTranslation>");
        }

        if (obj->m_SelectedKey)
        {
            buf.append("<SelectedKey>");
            buf.append(obj->m_SelectedKey->str());
            buf.append("</SelectedKey>");
        }

        return 0;
    }

    int serialize(DFUSpaceRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_CountBy)
        {
            buf.append("<CountBy>");
            buf.append(obj->m_CountBy->str());
            buf.append("</CountBy>");
        }

        if (obj->m_ScopeUnder)
        {
            buf.append("<ScopeUnder>");
            buf.append(obj->m_ScopeUnder->str());
            buf.append("</ScopeUnder>");
        }

        if (obj->m_OwnerUnder)
        {
            buf.append("<OwnerUnder>");
            buf.append(obj->m_OwnerUnder->str());
            buf.append("</OwnerUnder>");
        }

        if (obj->m_Interval)
        {
            buf.append("<Interval>");
            buf.append(obj->m_Interval->str());
            buf.append("</Interval>");
        }

        if (obj->m_StartDate)
        {
            buf.append("<StartDate>");
            buf.append(obj->m_StartDate->str());
            buf.append("</StartDate>");
        }

        if (obj->m_EndDate)
        {
            buf.append("<EndDate>");
            buf.append(obj->m_EndDate->str());
            buf.append("</EndDate>");
        }

        return 0;
    }

    int serialize(DFUSpaceItem* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_NumOfFiles)
        {
            buf.append("<NumOfFiles>");
            buf.append(obj->m_NumOfFiles->str());
            buf.append("</NumOfFiles>");
        }

        if (obj->m_NumOfFilesUnknown)
        {
            buf.append("<NumOfFilesUnknown>");
            buf.append(obj->m_NumOfFilesUnknown->str());
            buf.append("</NumOfFilesUnknown>");
        }

        if (obj->m_TotalSize)
        {
            buf.append("<TotalSize>");
            buf.append(obj->m_TotalSize->str());
            buf.append("</TotalSize>");
        }

        if (obj->m_LargestFile)
        {
            buf.append("<LargestFile>");
            buf.append(obj->m_LargestFile->str());
            buf.append("</LargestFile>");
        }

        if (obj->m_LargestSize)
        {
            buf.append("<LargestSize>");
            buf.append(obj->m_LargestSize->str());
            buf.append("</LargestSize>");
        }

        if (obj->m_SmallestFile)
        {
            buf.append("<SmallestFile>");
            buf.append(obj->m_SmallestFile->str());
            buf.append("</SmallestFile>");
        }

        if (obj->m_SmallestSize)
        {
            buf.append("<SmallestSize>");
            buf.append(obj->m_SmallestSize->str());
            buf.append("</SmallestSize>");
        }

        if (obj->m_NumOfFilesInt64)
        {
            buf.append("<NumOfFilesInt64>");
            buf.append(obj->m_NumOfFilesInt64->str());
            buf.append("</NumOfFilesInt64>");
        }

        if (obj->m_NumOfFilesUnknownInt64)
        {
            buf.append("<NumOfFilesUnknownInt64>");
            buf.append(obj->m_NumOfFilesUnknownInt64->str());
            buf.append("</NumOfFilesUnknownInt64>");
        }

        if (obj->m_TotalSizeInt64)
        {
            buf.append("<TotalSizeInt64>");
            buf.append(obj->m_TotalSizeInt64->str());
            buf.append("</TotalSizeInt64>");
        }

        if (obj->m_LargestSizeInt64)
        {
            buf.append("<LargestSizeInt64>");
            buf.append(obj->m_LargestSizeInt64->str());
            buf.append("</LargestSizeInt64>");
        }

        if (obj->m_SmallestSizeInt64)
        {
            buf.append("<SmallestSizeInt64>");
            buf.append(obj->m_SmallestSizeInt64->str());
            buf.append("</SmallestSizeInt64>");
        }

        return 0;
    }

    int serialize(DFUSpaceResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_CountBy)
        {
            buf.append("<CountBy>");
            buf.append(obj->m_CountBy->str());
            buf.append("</CountBy>");
        }

        if (obj->m_ScopeUnder)
        {
            buf.append("<ScopeUnder>");
            buf.append(obj->m_ScopeUnder->str());
            buf.append("</ScopeUnder>");
        }

        if (obj->m_OwnerUnder)
        {
            buf.append("<OwnerUnder>");
            buf.append(obj->m_OwnerUnder->str());
            buf.append("</OwnerUnder>");
        }

        if (obj->m_Interval)
        {
            buf.append("<Interval>");
            buf.append(obj->m_Interval->str());
            buf.append("</Interval>");
        }

        if (obj->m_StartDate)
        {
            buf.append("<StartDate>");
            buf.append(obj->m_StartDate->str());
            buf.append("</StartDate>");
        }

        if (obj->m_EndDate)
        {
            buf.append("<EndDate>");
            buf.append(obj->m_EndDate->str());
            buf.append("</EndDate>");
        }

        if (obj->m_DFUSpaceItems.length() > 0)
        {
            buf.append("<DFUSpaceItems>");
            ForEachItemIn(i, obj->m_DFUSpaceItems)
            {
                    DFUSpaceItem& oneitem = obj->m_DFUSpaceItems.item(i);
                buf.append("<>");
                serialize(&oneitem, buf);
                buf.append("</>");

            }
            buf.append("</DFUSpaceItems>");
        }

        return 0;
    }

    int serialize(EclRecordTypeInfoRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Ecl)
        {
            buf.append("<Ecl>");
            buf.append(obj->m_Ecl->str());
            buf.append("</Ecl>");
        }

        if (obj->m_IncludeJsonTypeInfo)
        {
            buf.append("<IncludeJsonTypeInfo>");
            buf.append(obj->m_IncludeJsonTypeInfo->str());
            buf.append("</IncludeJsonTypeInfo>");
        }

        if (obj->m_IncludeBinTypeInfo)
        {
            buf.append("<IncludeBinTypeInfo>");
            buf.append(obj->m_IncludeBinTypeInfo->str());
            buf.append("</IncludeBinTypeInfo>");
        }

        return 0;
    }

    int serialize(EclRecordTypeInfoResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_jsonInfo)
        {
            buf.append("<jsonInfo>");
            buf.append(obj->m_jsonInfo->str());
            buf.append("</jsonInfo>");
        }

        if (obj->m_binInfo)
        {
            buf.append("<binInfo>");
            buf.append(obj->m_binInfo->str());
            buf.append("</binInfo>");
        }

        return 0;
    }

    int serialize(EraseHistoryRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        return 0;
    }

    int serialize(History* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        if (obj->m_Operation)
        {
            buf.append("<Operation>");
            buf.append(obj->m_Operation->str());
            buf.append("</Operation>");
        }

        if (obj->m_Timestamp)
        {
            buf.append("<Timestamp>");
            buf.append(obj->m_Timestamp->str());
            buf.append("</Timestamp>");
        }

        if (obj->m_IP)
        {
            buf.append("<IP>");
            buf.append(obj->m_IP->str());
            buf.append("</IP>");
        }

        if (obj->m_Path)
        {
            buf.append("<Path>");
            buf.append(obj->m_Path->str());
            buf.append("</Path>");
        }

        if (obj->m_Owner)
        {
            buf.append("<Owner>");
            buf.append(obj->m_Owner->str());
            buf.append("</Owner>");
        }

        if (obj->m_Workunit)
        {
            buf.append("<Workunit>");
            buf.append(obj->m_Workunit->str());
            buf.append("</Workunit>");
        }

        return 0;
    }

    int serialize(EraseHistoryResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_xmlmap)
        {
            buf.append("<xmlmap>");
            buf.append(obj->m_xmlmap->str());
            buf.append("</xmlmap>");
        }

        if (obj->m_History.length() > 0)
        {
            buf.append("<History>");
            ForEachItemIn(i, obj->m_History)
            {
                    History& oneitem = obj->m_History.item(i);
                buf.append("<Origin>");
                serialize(&oneitem, buf);
                buf.append("</Origin>");

            }
            buf.append("</History>");
        }

        return 0;
    }

    int serialize(ListHistoryRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_Name)
        {
            buf.append("<Name>");
            buf.append(obj->m_Name->str());
            buf.append("</Name>");
        }

        return 0;
    }

    int serialize(ListHistoryResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_xmlmap)
        {
            buf.append("<xmlmap>");
            buf.append(obj->m_xmlmap->str());
            buf.append("</xmlmap>");
        }

        if (obj->m_History.length() > 0)
        {
            buf.append("<History>");
            ForEachItemIn(i, obj->m_History)
            {
                    History& oneitem = obj->m_History.item(i);
                buf.append("<Origin>");
                serialize(&oneitem, buf);
                buf.append("</Origin>");

            }
            buf.append("</History>");
        }

        return 0;
    }

    int serialize(WsDfuPingRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(WsDfuPingResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        return 0;
    }

    int serialize(SavexmlRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_name)
        {
            buf.append("<name>");
            buf.append(obj->m_name->str());
            buf.append("</name>");
        }

        return 0;
    }

    int serialize(SavexmlResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_xmlmap)
        {
            buf.append("<xmlmap>");
            buf.append(obj->m_xmlmap->str());
            buf.append("</xmlmap>");
        }

        return 0;
    }

    int serialize(SuperfileActionRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_action)
        {
            buf.append("<action>");
            buf.append(obj->m_action->str());
            buf.append("</action>");
        }

        if (obj->m_superfile)
        {
            buf.append("<superfile>");
            buf.append(obj->m_superfile->str());
            buf.append("</superfile>");
        }

        if (obj->m_subfiles.length() > 0)
        {
            buf.append("<subfiles>");
            ForEachItemIn(i, obj->m_subfiles)
            {
            PString& oneitem = obj->m_subfiles.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</subfiles>");
        }

        if (obj->m_before)
        {
            buf.append("<before>");
            buf.append(obj->m_before->str());
            buf.append("</before>");
        }

        if (obj->m_delete)
        {
            buf.append("<delete>");
            buf.append(obj->m_delete->str());
            buf.append("</delete>");
        }

        if (obj->m_removeSuperfile)
        {
            buf.append("<removeSuperfile>");
            buf.append(obj->m_removeSuperfile->str());
            buf.append("</removeSuperfile>");
        }

        return 0;
    }

    int serialize(SuperfileActionResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_superfile)
        {
            buf.append("<superfile>");
            buf.append(obj->m_superfile->str());
            buf.append("</superfile>");
        }

        if (obj->m_retcode)
        {
            buf.append("<retcode>");
            buf.append(obj->m_retcode->str());
            buf.append("</retcode>");
        }

        return 0;
    }

    int serialize(SuperfileListRequest* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_superfile)
        {
            buf.append("<superfile>");
            buf.append(obj->m_superfile->str());
            buf.append("</superfile>");
        }

        return 0;
    }

    int serialize(SuperfileListResponse* obj, StringBuffer& buf)
    {
        if (!obj)
            return 0;

        if (obj->m_superfile)
        {
            buf.append("<superfile>");
            buf.append(obj->m_superfile->str());
            buf.append("</superfile>");
        }

        if (obj->m_subfiles.length() > 0)
        {
            buf.append("<subfiles>");
            ForEachItemIn(i, obj->m_subfiles)
            {
            PString& oneitem = obj->m_subfiles.item(i);
                buf.append("<>").append(oneitem.str()).append("</>");

            }
            buf.append("</subfiles>");
        }

        return 0;
    }

};
int WsDfuServiceBase::Add(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<AddRequest> req = new AddRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<AddResponse> resp = service->Add(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"AddResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::AddRemote(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<AddRemoteRequest> req = new AddRemoteRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<AddRemoteResponse> resp = service->AddRemote(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"AddRemoteResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::AddtoSuperfile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<AddtoSuperfileRequest> req = new AddtoSuperfileRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<AddtoSuperfileResponse> resp = service->AddtoSuperfile(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"AddtoSuperfileResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUArrayAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUArrayActionRequest> req = new DFUArrayActionRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUArrayActionResponse> resp = service->DFUArrayAction(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUArrayActionResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUBrowseData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUBrowseDataRequest> req = new DFUBrowseDataRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUBrowseDataResponse> resp = service->DFUBrowseData(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUBrowseDataResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUDefFile(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUDefFileRequest> req = new DFUDefFileRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUDefFileResponse> resp = service->DFUDefFile(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUDefFileResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFileAccess(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFileAccessRequest> req = new DFUFileAccessRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFileAccessResponse> resp = service->DFUFileAccess(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFileAccessResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFileAccessV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFileAccessV2Request> req = new DFUFileAccessV2Request();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFileAccessResponse> resp = service->DFUFileAccessV2(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFileAccessResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFileCreate(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFileCreateRequest> req = new DFUFileCreateRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFileCreateResponse> resp = service->DFUFileCreate(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFileCreateResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFileCreateV2(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFileCreateV2Request> req = new DFUFileCreateV2Request();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFileCreateResponse> resp = service->DFUFileCreateV2(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFileCreateResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFilePublish(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFilePublishRequest> req = new DFUFilePublishRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFilePublishResponse> resp = service->DFUFilePublish(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFilePublishResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUFileView(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUFileViewRequest> req = new DFUFileViewRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUFileViewResponse> resp = service->DFUFileView(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUFileViewResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUGetDataColumns(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUGetDataColumnsRequest> req = new DFUGetDataColumnsRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUGetDataColumnsResponse> resp = service->DFUGetDataColumns(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUGetDataColumnsResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUGetFileMetaData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUGetFileMetaDataRequest> req = new DFUGetFileMetaDataRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUGetFileMetaDataResponse> resp = service->DFUGetFileMetaData(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUGetFileMetaDataResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUInfoRequest> req = new DFUInfoRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUInfoResponse> resp = service->DFUInfo(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUInfoResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUQuery(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUQueryRequest> req = new DFUQueryRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUQueryResponse> resp = service->DFUQuery(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUQueryResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFURecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFURecordTypeInfoRequest> req = new DFURecordTypeInfoRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFURecordTypeInfoResponse> resp = service->DFURecordTypeInfo(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFURecordTypeInfoResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUSearch(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUSearchRequest> req = new DFUSearchRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUSearchResponse> resp = service->DFUSearch(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUSearchResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUSearchData(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUSearchDataRequest> req = new DFUSearchDataRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUSearchDataResponse> resp = service->DFUSearchData(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUSearchDataResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::DFUSpace(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<DFUSpaceRequest> req = new DFUSpaceRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<DFUSpaceResponse> resp = service->DFUSpace(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"DFUSpaceResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::EclRecordTypeInfo(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<EclRecordTypeInfoRequest> req = new EclRecordTypeInfoRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<EclRecordTypeInfoResponse> resp = service->EclRecordTypeInfo(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"EclRecordTypeInfoResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::EraseHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<EraseHistoryRequest> req = new EraseHistoryRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<EraseHistoryResponse> resp = service->EraseHistory(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"EraseHistoryResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::ListHistory(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<ListHistoryRequest> req = new ListHistoryRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<ListHistoryResponse> resp = service->ListHistory(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"ListHistoryResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::Ping(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<WsDfuPingRequest> req = new WsDfuPingRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<WsDfuPingResponse> resp = service->Ping(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"WsDfuPingResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::Savexml(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<SavexmlRequest> req = new SavexmlRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<SavexmlResponse> resp = service->Savexml(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"SavexmlResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::SuperfileAction(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<SuperfileActionRequest> req = new SuperfileActionRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<SuperfileActionResponse> resp = service->SuperfileAction(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"SuperfileActionResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
int WsDfuServiceBase::SuperfileList(const char* CtxStr, const char* ReqStr, StringBuffer& RespStr)
{
    Owned<WsDfuUnSerializer> UnSe = new WsDfuUnSerializer();
    Owned<EsdlContext> ctx = new EsdlContext();
    if (CtxStr && *CtxStr)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(CtxStr);
        UnSe->unserialize(ctx.get(), ptree.get());
    }
    Owned<WsDfuServiceBase> service = createWsDfuServiceObj();
    Owned<SuperfileListRequest> req = new SuperfileListRequest();
    Owned<IPropertyTree> ptree;
    ptree.setown(createPTreeFromXMLString(ReqStr));
    UnSe->unserialize(req.get(), ptree.get());
    Owned<SuperfileListResponse> resp = service->SuperfileList(ctx.get(), req.get());
    RespStr = "<Response><Results><Result><Dataset name=\"SuperfileListResponse\"><Row>";
    UnSe->serialize(resp.get(), RespStr);
    RespStr.append("</Row></Dataset></Result></Results></Response>");

    return 0;
}
