/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include "jlib.hpp"
#include "jfile.hpp"
#include "jprop.hpp"

//------------------------------------------------------------------------------
//This following code is for testing log dataset inside origResp.
//Copy the code to esdl_binding.cpp.

#ifdef USE_TEST_RESP
                LOG(LegacyMsgCatMin,"origResp: %s", origResp.str());
                StringBuffer respWithLodData;
                respWithLodData.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?><soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body><ExchangeNumbersResponse xmlns=\"urn:hpccsystems:ecl:exchangenumbers\" sequence=\"0\"><Results><Result>\n");
                respWithLodData.append("<Dataset xmlns=\"urn:hpccsystems:ecl:vin_services.servicevinstandard:result:log_log__vin_intermediate__log\" name=\"LOG_log__vin_intermediate__log\">\n");
                respWithLodData.append(" <Row><Records><Rec><Content_Type>InsuranceContext</Content_Type><Version>1</Version><Fields/><product_id>1</product_id></Rec></Records></Row>\n");
                respWithLodData.append("</Dataset>\n");
                respWithLodData.append("<Dataset xmlns=\"urn:hpccsystems:ecl:vin_services.servicevinstandard:result:log_log__vin_transaction__log\" name=\"LOG_log__vin_transaction__log\">\n");
                respWithLodData.append(" <Row><Records><Rec><price>-1</price><free>0</free><order_status_code>402</order_status_code><country_address>USA</country_address><response_time>0</response_time></Rec></Records></Row>\n");
                respWithLodData.append("</Dataset>\n");
                respWithLodData.append("<Dataset xmlns='urn:hpccsystems:ecl:exchangenumbers:result:exchangenumbersresponse' name='ExchangeNumbersResponse'>\n");
                respWithLodData.append(" <Row><Description>[&#xe01a;] back</Description><NumberListOut><number1>2</number1><number2>1</number2></NumberListOut><NumberListOut><number1>2</number1><number2>1</number2></NumberListOut><StringListOut>back</StringListOut><StringListOut> back</StringListOut><NumberArrayOut><TwoNumbers><number1>2</number1><number2>1</number2></TwoNumbers><TwoNumbers><number1>2</number1><number2>1</number2></TwoNumbers></NumberArrayOut><StringArrayOut><MyString> back</MyString><MyString>&#xe01a;&#xe01a; back</MyString></StringArrayOut></Row>\n");
                respWithLodData.append("</Dataset>\n");
                respWithLodData.append("</Result></Results></ExchangeNumbersResponse></soap:Body></soap:Envelope>");
                Owned<IXmlWriterExt> respWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
                m_pEsdlTransformer->processHPCCResult(context, mthdef, respWithLodData.str(), respWriter.get(), logdata, ESDL_TRANS_OUTPUT_ROOT, ns, schema_location);
#else
//                Owned<IXmlWriterExt> respWriter = createIXmlWriterExt(0, 0, NULL, (flags & ESDL_BINDING_RESPONSE_JSON) ? WTJSONRootless : WTStandard);
//                m_pEsdlTransformer->processHPCCResult(context, mthdef, origResp.str(), respWriter.get(), logdata, ESDL_TRANS_OUTPUT_ROOT, ns, schema_location);
#endif
//------------------------------------------------------------------------------

void usage()
{
    puts("Usage:");
    puts("   logging_test [optins]");
    puts("Options:");
    puts("   -i <filename> : input file name using for both config and request");
    puts("   -o <filename> : out file name");
    puts("   -action <soap-action> ");
    puts("");
    puts("sample config-file: ");
    puts("<logging>");
    puts("  <config>");
    puts("    <LoggingManager>");
    puts("      <LogAgent name=\"ESPLoggingAgent\" type=\"LogAgent\" services=\"GetTransactionSeed,UpdateLog\" plugin=\"espserverloggingagent\">");
    puts("        <url>http://10.176.152.168:7501</url>");
    puts("        ... ...");
    puts("        <Filters>");
    puts("          <Filter value=\"Context\"/>");
    puts("          <Filter value=\"Request\"/>");
    puts("        </Filters>");
    puts("      </LogAgent>");
    puts("    </LoggingManager>");
    puts("  </config>");
    puts("  <test>");
    puts("    <action>UpdateLog</action>");
    puts("    <option>SingleInsert</option>");
    puts("    <LogContent>");
    puts("      <Context>");
    puts("        <UserID>myUserID1</UserID>");
    puts("        <IPAddress>10.10.10.10</IPAddress>");
    puts("        <FunctionName>Search Movies</FunctionName>");
    puts("        <NotForLog>abc</NotForLog>");
    puts("      </Context>");
    puts("      <Request>");
    puts("        <Company>ALL Movies</Company>");
    puts("        <Address>101 1st Ave., LA, CA 12345</Address>");
    puts("      </Request>");
    puts("      <Response>");
    puts("        <RecordCount>2</RecordCount>");
    puts("        <Dataset name=\"GENERIC_LOG_DATA\">");
    puts("          <Rec>");
    puts("            <Data1>data 1</Data1>");
    puts("            <Data2>2</Data2>");
    puts("          </Rec>");
    puts("          <Test2>2</Test2>");
    puts("        </Dataset>");
    puts("      </Response>");
    puts("    </LogContent>");
    puts("  </test>");
    puts("</logging>");
    puts("");

    exit(-1);
}

void doWork(const char* inputFileName);

int main(int argc, char* argv[])
{
    InitModuleObjects();

    StringBuffer in_fname;
    StringBuffer out_fname;

    int i = 1;
    while(i<argc)
    {
        if (stricmp(argv[i], "-i") == 0)
        {
            i++;
            in_fname.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i], "-o") == 0)
        {
            i++;
            out_fname.clear().append(argv[i++]);
        }
        else
        {
            printf("Error: command format error\n");
            usage();
            exit(-1);
        }
    }

    try
    {
        FILE* ofile = NULL;
        if(out_fname.length() != 0)
        {
            ofile = fopen(out_fname.str(), "a+");
            if(ofile == NULL)
            {
                printf("can't open file %s\n", out_fname.str());
                exit(-1);
            }
        }
        else
        {
            ofile = stdout;
        }

        doWork(in_fname.str());
        printf("Finished\n");
        fclose(ofile);
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        printf("Error - %d:%s\n", excpt->errorCode(), excpt->errorMessage(errMsg).str());
        return -1;
    }
    catch(...)
    {
        printf("Unknown exception\n");
        return -1;
    }

    releaseAtoms();
    return 0;
}
