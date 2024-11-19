/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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
#ifdef _USE_CPPUNIT
#include <sstream>
#include "jfile.hpp"
#include "jscm.hpp"
#include "unittests.hpp"
#include "espapi.hpp"
#include "jfile.hpp"
#include <iostream>
#include <set>

using namespace std;

static constexpr const char* commonXML = R"!!(<esxdl name="common">
    <EsdlStruct name='NamedValue' nil_remove='1'>
        <EsdlElement name='Name' type='string'/>
        <EsdlElement name='Value' type='string'/>
    </EsdlStruct>
    <EsdlStruct name='NameAndType' nil_remove='1'>
        <EsdlElement name='Name' type='string'/>
        <EsdlElement name='Type' type='string'/>
    </EsdlStruct>
    <EsdlStruct name='LogicalFileError' nil_remove='1'>
        <EsdlElement name='Error' type='string'/>
        <EsdlElement name='LogicalName' type='string'/>
    </EsdlStruct>
</esxdl>)!!";

static constexpr const char* storeXML = R"!!(<esxdl name="ws_store">
    <EsdlStruct name='KVPair'>
        <EsdlElement name='Key' type='string'/>
        <EsdlElement name='Value' type='string'/>
    </EsdlStruct>
    <EsdlStruct name='NamespaceSet'>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlArray name='Entries'  type='KVPair' item_tag='Entry'/>
    </EsdlStruct>
    <EsdlStruct name='StoreInfo'>
        <EsdlElement name='Name' type='string'/>
        <EsdlElement name='Type' type='string'/>
        <EsdlElement name='Description' type='string'/>
        <EsdlElement name='Owner' type='string'/>
        <EsdlElement name='CreateTime' type='string'/>
        <EsdlElement name='MaxValSize' type='string'/>
        <EsdlElement name='IsDefault' type='bool' default='0'/>
    </EsdlStruct>
    <EsdlRequest name='ListKeysRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlResponse name='ListKeysResponse' exceptions_inline='1'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlArray name='KeySet'  type='string' item_tag='Key'/>
    </EsdlResponse>
    <EsdlRequest name='ListStoresRequest'>
        <EsdlElement name='NameFilter' type='string'/>
        <EsdlElement name='TypeFilter' type='string'/>
        <EsdlElement name='OwnerFilter' type='string'/>
    </EsdlRequest>
    <EsdlResponse name='ListStoresResponse' exceptions_inline='1'>
        <EsdlArray name='Stores'  type='StoreInfo' item_tag='Store'/>
    </EsdlResponse>
    <EsdlRequest name='ListNamespacesRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlResponse name='ListNamespacesResponse' exceptions_inline='1'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlArray name='Namespaces'  type='string' item_tag='Namespace'/>
    </EsdlResponse>
    <EsdlRequest name='DeleteNamespaceRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
        <EsdlElement name='TargetUser' type='string'/>
    </EsdlRequest>
    <EsdlResponse name='DeleteNamespaceResponse' exceptions_inline='1'>
        <EsdlElement name='Success' type='bool'/>
    </EsdlResponse>
    <EsdlRequest name='DeleteRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='Key' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
        <EsdlElement name='TargetUser' type='string'/>
    </EsdlRequest>
    <EsdlResponse name='DeleteResponse' exceptions_inline='1'>
        <EsdlElement name='Success' type='bool'/>
    </EsdlResponse>
    <EsdlRequest name='FetchRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='Key' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlResponse name='FetchResponse' exceptions_inline='1'>
        <EsdlElement name='Value' type='string' nil_remove='1'/>
    </EsdlResponse>
    <EsdlRequest name='FetchAllRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlRequest name='FetchKeyMDRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='Key' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlResponse name='FetchKeyMDResponse' exceptions_inline='1'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='Key' type='string'/>
        <EsdlArray name='Pairs'  type='KVPair' item_tag='Pair'/>
    </EsdlResponse>
    <EsdlResponse name='FetchAllResponse' exceptions_inline='1'>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlArray name='Pairs'  type='KVPair' item_tag='Pair'/>
    </EsdlResponse>
    <EsdlRequest name='SetRequest'>
        <EsdlElement name='StoreName' type='string'/>
        <EsdlElement name='Namespace' type='string'/>
        <EsdlElement name='Key' type='string'/>
        <EsdlElement name='Value' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0'/>
    </EsdlRequest>
    <EsdlResponse name='SetResponse' exceptions_inline='1'>
        <EsdlElement name='Success' type='bool'/>
    </EsdlResponse>
    <EsdlRequest name='CreateStoreRequest'>
        <EsdlElement name='Name' type='string'/>
        <EsdlElement name='Type' type='string'/>
        <EsdlElement name='Description' type='string'/>
        <EsdlElement name='UserSpecific' type='bool' default='0' depr_ver='1.01'/>
        <EsdlElement name='MaxValueSize' type='int' default='1024' min_ver='1.02'/>
    </EsdlRequest>
    <EsdlResponse name='CreateStoreResponse' exceptions_inline='1'>
        <EsdlElement name='Name' type='string'/>
        <EsdlElement name='Type' type='string'/>
        <EsdlElement name='Description' type='string'/>
        <EsdlElement name='Owner' type='string'/>
        <EsdlElement name='Success' type='bool' min_ver='1.01'/>
    </EsdlResponse>
    <EsdlRequest name='wsstorePingRequest'>
    </EsdlRequest>
    <EsdlResponse name='wsstorePingResponse'>
    </EsdlResponse>
        <EsdlService name='wsstore'  exceptions_inline='./smc_xslt/exceptions.xslt' auth_feature='WsStoreAccess:READ' default_client_version='1.02' version='1.02'>
    <EsdlMethod name='CreateStore' request_type='CreateStoreRequest' response_type='CreateStoreResponse'  auth_feature='WsStoreAccess:FULL'/>
    <EsdlMethod name='Delete' request_type='DeleteRequest' response_type='DeleteResponse'  auth_feature='WsStoreAccess:FULL'/>
    <EsdlMethod name='DeleteNamespace' request_type='DeleteNamespaceRequest' response_type='DeleteNamespaceResponse'  auth_feature='WsStoreAccess:FULL'/>
    <EsdlMethod name='Fetch' request_type='FetchRequest' response_type='FetchResponse'  auth_feature='WsStoreAccess:READ'/>
    <EsdlMethod name='FetchAll' request_type='FetchAllRequest' response_type='FetchAllResponse'  auth_feature='WsStoreAccess:READ'/>
    <EsdlMethod name='FetchKeyMetadata' request_type='FetchKeyMDRequest' response_type='FetchKeyMDResponse'  auth_feature='WsStoreAccess:FULL'/>
    <EsdlMethod name='ListKeys' request_type='ListKeysRequest' response_type='ListKeysResponse'  auth_feature='WsStoreAccess:READ'/>
    <EsdlMethod name='ListNamespaces' request_type='ListNamespacesRequest' response_type='ListNamespacesResponse'  auth_feature='WsStoreAccess:READ'/>
    <EsdlMethod name='ListStores' request_type='ListStoresRequest' response_type='ListStoresResponse'  min_ver='1.02' auth_feature='WsStoreAccess:READ'/>
    <EsdlMethod name='Ping' request_type='wsstorePingRequest' response_type='wsstorePingResponse'  auth_feature='none'/>
    <EsdlMethod name='Set' request_type='SetRequest' response_type='SetResponse'  auth_feature='WsStoreAccess:WRITE'/>
        </EsdlService></esxdl>)!!";

static constexpr const char* sashaXML = R"!!(<esxdl name="ws_sasha">
    <EsdlInclude file='common'/>
    <EsdlEnumType name='WUTypes' base_type='string'>
        <EsdlEnumItem name='ECL' enum='ECL'/>
        <EsdlEnumItem name='DFU' enum='DFU'/>
    </EsdlEnumType>
    <EsdlRequest name='GetVersionRequest' nil_remove='1'>
    </EsdlRequest>
    <EsdlResponse name='ResultResponse' nil_remove='1' exceptions_inline='1'>
        <EsdlElement name='Result' type='string'/>
    </EsdlResponse>
    <EsdlRequest name='ArchiveWURequest' nil_remove='1'>
        <EsdlElement name='Wuid' type='string'/>
        <EsdlEnum name='WUType' enum_type='WUTypes'/>
        <EsdlElement name='DeleteOnSuccess' type='bool' default='1'/>
    </EsdlRequest>
    <EsdlRequest name='RestoreWURequest' nil_remove='1'>
        <EsdlElement name='Wuid' type='string'/>
        <EsdlEnum name='WUType' enum_type='WUTypes'/>
    </EsdlRequest>
    <EsdlRequest name='ListWURequest' nil_remove='1'>
        <EsdlEnum name='WUType' enum_type='WUTypes'/>
        <EsdlElement name='Wuid' type='string'/>
        <EsdlElement name='Cluster' type='string'/>
        <EsdlElement name='Owner' type='string'/>
        <EsdlElement name='JobName' type='string'/>
        <EsdlElement name='State' type='string'/>
        <EsdlElement name='FromDate' type='string'/>
        <EsdlElement name='ToDate' type='string'/>
        <EsdlElement name='Archived' type='bool' default='0'/>
        <EsdlElement name='Online' type='bool' default='0'/>
        <EsdlElement name='IncludeDT' type='bool' default='0'/>
        <EsdlElement name='BeforeWU' type='string'/>
        <EsdlElement name='AfterWU' type='string'/>
        <EsdlElement name='MaxNumberWUs' type='unsigned' default='500'/>
        <EsdlElement name='Descending' type='bool' default='0'/>
        <EsdlElement name='OutputFields' type='string'/>
    </EsdlRequest>
    <EsdlRequest name='WSSashaPingRequest'>
    </EsdlRequest>
    <EsdlResponse name='WSSashaPingResponse'>
    </EsdlResponse>
        <EsdlService name='WSSasha'  exceptions_inline='./smc_xslt/exceptions.xslt' default_client_version='1.01' version='1.01' auth_feature='DEFERRED'>
    <EsdlMethod name='ArchiveWU' request_type='ArchiveWURequest' response_type='ResultResponse'  auth_feature='SashaAccess:FULL'/>
    <EsdlMethod name='GetVersion' request_type='GetVersionRequest' response_type='ResultResponse'  auth_feature='SashaAccess:Access'/>
    <EsdlMethod name='ListWU' request_type='ListWURequest' response_type='ResultResponse'  min_ver='1.01' auth_feature='SashaAccess:READ'/>
    <EsdlMethod name='Ping' request_type='WSSashaPingRequest' response_type='WSSashaPingResponse'  auth_feature='none'/>
    <EsdlMethod name='RestoreWU' request_type='RestoreWURequest' response_type='ResultResponse'  auth_feature='SashaAccess:FULL'/>
        </EsdlService></esxdl>)!!";

class EspApiCmdTest : public CppUnit::TestFixture
{
    class TestEspDef : public EspDef
    {
    public:
        const std::vector<const char*>& getAllServices()
        {
            return allServicesList;
        }

        const std::vector<const char*>& getAllMethods()
        {
            return allMethodsList;
        }
    };

    struct Environment
    {
        TestEspDef   esdlDefObj;
        std::vector<Owned<IFile>> files;
        Owned<IFile> dir;
        Owned<IFile> common;
        Owned<IFile> store;
        Owned<IFile> sasha;

        Environment()
           :dir(createIFile("/tmp/hpcctest")),
            common(createIFile("/tmp/hpcctest/common.xml")),
            store(createIFile("/tmp/hpcctest/store.xml")),
            sasha(createIFile("/tmp/hpcctest/sasha.xml"))
        {
            try{
                dir->createDirectory();

                Owned<IFileIO> ioCommon = common->open(IFOcreate);
                ioCommon->write(0, strlen(commonXML), commonXML);

                Owned<IFileIO> ioStore = store->open(IFOcreate);
                ioStore->write(0, strlen(storeXML), storeXML);

                Owned<IFileIO> ioSasha = sasha->open(IFOcreate);
                ioSasha->write(0, strlen(sashaXML), sashaXML);

                esdlDefObj.getFiles(files,"/tmp/hpcctest");
                esdlDefObj.addFilesToDefinition(files);
                esdlDefObj.loadAllServices();
                esdlDefObj.loadAllMethods("WSSasha");
            }catch(IException * e){
                StringBuffer msg;
                e->errorMessage(msg);
                cerr << msg.str();
            }
        }
        ~Environment()
        {
            common->remove();
            store->remove();
            sasha->remove();
            dir->remove();
        }
    };

public:
    EspApiCmdTest()
    {
    }

    CPPUNIT_TEST_SUITE(EspApiCmdTest);
        CPPUNIT_TEST(testFileLoad);
        CPPUNIT_TEST(testLoadAllServices);
        CPPUNIT_TEST(testDescribeAllServices);
        CPPUNIT_TEST(testPrintAllMethods);
        CPPUNIT_TEST(testDescribeAllMethods);
        CPPUNIT_TEST(testCheckValidService);
        CPPUNIT_TEST(testCheckValidMethod);
        CPPUNIT_TEST(testDescribe);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testFileLoad()
    {
        string fileStr;
        string refStr =
        "/tmp/hpcctest/sasha.xml\n"
        "/tmp/hpcctest/common.xml\n"
        "/tmp/hpcctest/store.xml\n";

        for(Owned<IFile>& myfile: queryEnvironment().files)
        {
            fileStr.append(myfile->queryFilename());
            fileStr.append("\n");
        }

        sortAndWriteBack(fileStr);
        sortAndWriteBack(refStr);
        CPPUNIT_ASSERT_EQUAL(fileStr, refStr);
    }
    void testLoadAllServices()
    {
        string serviceStr;
        string refStr =
        "WSSasha\n"
        "wsstore\n";
        vector<const char*> services = queryEnvironment().esdlDefObj.getAllServices();
        for(const char* service : services)
        {
            serviceStr.append(service);
            serviceStr.append("\n");
        }
        sortAndWriteBack(serviceStr);
        sortAndWriteBack(refStr);
        CPPUNIT_ASSERT_EQUAL(serviceStr, refStr);
    }
    void testDescribeAllServices()
    {
        string refStr =
        "WSSasha [version (1.01), auth_feature (DEFERRED), exceptions_inline (./smc_xslt/exceptions.xslt), default_client_version (1.01)]\n"
        "wsstore [version (1.02), auth_feature (WsStoreAccess:READ), exceptions_inline (./smc_xslt/exceptions.xslt), default_client_version (1.02)]\n";
        ostringstream oss;
        string res;

        queryEnvironment().esdlDefObj.describeAllServices(oss);
        res = oss.str();

        sortAndWriteBack(res);
        sortAndWriteBack(refStr);
        CPPUNIT_ASSERT_EQUAL(res, refStr);
    }
    void testPrintAllMethods()
    {
        string methodStr;
        string refStr =
        "ArchiveWU\n"
        "GetVersion\n"
        "ListWU\n"
        "Ping\n"
        "RestoreWU\n";

        vector<const char*> methods = queryEnvironment().esdlDefObj.getAllMethods();
        for(const char* &method:methods)
        {
            methodStr.append(method);
            methodStr.append("\n");
        }
        CPPUNIT_ASSERT_EQUAL(methodStr, refStr);
    }
    void testDescribeAllMethods()
    {
        string refStr =
        "WSSasha [version (1.01), auth_feature (DEFERRED), exceptions_inline (./smc_xslt/exceptions.xslt), default_client_version (1.01)]\n"
        "    ArchiveWU [response_type (ResultResponse), auth_feature (SashaAccess:FULL), request_type (ArchiveWURequest)]\n"
        "    GetVersion [response_type (ResultResponse), auth_feature (SashaAccess:Access), request_type (GetVersionRequest)]\n"
        "    ListWU [response_type (ResultResponse), auth_feature (SashaAccess:READ), request_type (ListWURequest), min_ver (1.01)]\n"
        "    Ping [response_type (WSSashaPingResponse), auth_feature (none), request_type (WSSashaPingRequest)]\n"
        "    RestoreWU [response_type (ResultResponse), auth_feature (SashaAccess:FULL), request_type (RestoreWURequest)]\n";
        ostringstream oss;
        string res;

        queryEnvironment().esdlDefObj.describeAllMethods("WSSasha",oss);
        res = oss.str();

        sortAndWriteBack(res);
        sortAndWriteBack(refStr);

        CPPUNIT_ASSERT_EQUAL(res, refStr);

    }
    void testCheckValidService()
    {
        bool validCheck = queryEnvironment().esdlDefObj.checkValidService("WSSasha");
        bool invalidCheck = queryEnvironment().esdlDefObj.checkValidService("InvalidService");
        CPPUNIT_ASSERT_EQUAL(validCheck, true);
        CPPUNIT_ASSERT_EQUAL(invalidCheck, false);
    }
    void testCheckValidMethod()
    {
        bool validCheck = queryEnvironment().esdlDefObj.checkValidMethod("GetVersion", "WSSasha");
        bool invalidCheck = queryEnvironment().esdlDefObj.checkValidMethod("InvalidMethod", "WSSasha");
        CPPUNIT_ASSERT_EQUAL(validCheck, true);
        CPPUNIT_ASSERT_EQUAL(invalidCheck, false);

    }
    void testDescribe()
    {
        queryEnvironment().esdlDefObj.loadAllMethods("wsstore");
        string refString =
        "ListStoresRequest\n"
        "string NameFilter\n"
        "string TypeFilter\n"
        "string OwnerFilter\n"
        "ListStoresResponse\n"
        "StoreInfo[] Stores [item_tag (Store)]\n"
        "    string Name\n"
        "    string Type\n"
        "    string Description\n"
        "    string Owner\n"
        "    string CreateTime\n"
        "    string MaxValSize\n"
        "    bool IsDefault [default (0)]\n";

        string refInvalidServiceString =
        "Invalid Service bad_name\n";

        string refInvalidMethodString =
        "Invalid Method bad_name\n";

        ostringstream oss;
        string res;
        queryEnvironment().esdlDefObj.describe("wsstore","ListStores", oss);
        res = oss.str();

        oss.str("");
        oss.clear();
        string invalidServiceRes;
        queryEnvironment().esdlDefObj.describe("bad_name","ListStores", oss);
        invalidServiceRes = oss.str();

        oss.str("");
        oss.clear();
        string invalidMethodRes;
        queryEnvironment().esdlDefObj.describe("wsstore","bad_name", oss);
        invalidMethodRes = oss.str();

        CPPUNIT_ASSERT_EQUAL(refString, res);
        CPPUNIT_ASSERT_EQUAL(refInvalidServiceString, invalidServiceRes);
        CPPUNIT_ASSERT_EQUAL(refInvalidMethodString, invalidMethodRes);
    }

private:
    Environment & queryEnvironment()
    {
        //Avoid creating this object until it is first used - otherwise any calls to the logging will crash
        static Environment env;
        return env;
    }

    void sortAndWriteBack(string& input)
    {
        stringstream ss(input);
        string token;
        char delimiter = '\n';
        set<string> sortedSet;

        while(getline(ss, token, delimiter))
        {
            sortedSet.insert(token);
        }
        input.clear();

        for(const string &s : sortedSet)
        {
            if(!input.empty())
            {
                input.append(" ");
            }
            input.append(s);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EspApiCmdTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EspApiCmdTest, "EspApiCmdTest" );

#endif
