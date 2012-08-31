/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include <stdlib.h>
#include "ScrubbedXMLService.hpp"




CEspScrubbedXmlService::CEspScrubbedXmlService()
{
}



CEspScrubbedXmlService::~CEspScrubbedXmlService()
{
}



bool CEspScrubbedXmlService::init(const char * name, const char * type, IPropertyTree * cfg, const char * process)
{
    return true;
}



bool CEspScrubbedXmlService::getHtmlForm(IEspContext &context, const char *path, const char *service, StringBuffer &formStr)
{
    formStr.append
    (
        
        "<br>SSN:<br>"
        "<input type=\"text\" name=\"SSN\">"
        "<br>First Name:<br>"
        "<input type=\"text\" name=\"FirstName\">"
        "<br>Last Name:<br>"
        "<input type=\"text\" name=\"LastName\">"
        "<br>Middle Name:<br>"
        "<input type=\"text\" name=\"MiddleName\">"
        "<br>State:<br>"
        "<input type=\"text\" name=\"State\">"
        "<br>City:<br>"
        "<input type=\"text\" name=\"City\">"
        "<br>Zip:<br>"
        "<input type=\"text\" name=\"Zip\">"
        "<br>Phone Number:<br>"
        "<input type=\"text\" name=\"Phone\">"
        "<br><br><br>"
        "<input type=\"submit\" value=\"Submit\">"
    );

    //throw MakeStringException(-1, "The requested service does not exist.");

    return true;
}



bool CEspScrubbedXmlService::getHtmlResults_Xslt(IEspContext &context, const char * path, const char * service, StringBuffer & xsltStr)
{
    xsltStr.append
    (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
            "<xsl:template match=\"/\">"
                "<html>"
                    "<head/>"
                    "<body>"
                        "<xsl:for-each select=\"//Dataset\">"
                            "<xsl:for-each select=\"Row\">"
                                "<xsl:if test=\"position()=1\">"
                                    "<xsl:text disable-output-escaping=\"yes\">&lt;table border=\"1\" cellspacing=\"0\" &gt;</xsl:text>"
                                    "<tr>"
                                        "<xsl:for-each select=\"*\">"
                                            "<th>"
                                                "<xsl:value-of select=\"name()\"/>"
                                            "</th>"
                                        "</xsl:for-each>"
                                    "</tr>"
                                "</xsl:if>"
                                "<tr>"
                                    "<xsl:for-each select=\"*\">"
                                        "<td align=\"center\">"
                                            "<xsl:value-of select=\".\"/>"
                                        "</td>"
                                    "</xsl:for-each>"
                                "</tr>"
                            "</xsl:for-each>"
                            "<xsl:if test=\"position()=last()\">"
                                "<xsl:text disable-output-escaping=\"yes\">&lt;/table&gt;</xsl:text>"
                            "</xsl:if>"
                        "</xsl:for-each>"
                    "</body>"
                "</html>"
            "</xsl:template>"
        "</xsl:stylesheet>"
    );

    return true;
}


bool CEspScrubbedXmlService::getWSDL(IEspContext &context, const char * path, const char * service, StringBuffer & wsdl)
{
    return false;
}


bool CEspScrubbedXmlService::getWSDL_Message(IEspContext &context, const char * path, const char * service, StringBuffer & wsdlMsg)
{
    return false;
}


bool CEspScrubbedXmlService::getWSDL_Schema(IEspContext &context, const char * path, const char * service, StringBuffer & wsdlSchema)
{
    return false;
}


bool CEspScrubbedXmlService::getResults_Schema(IEspContext &context, const char * path, const char * service, StringBuffer & dataSchema)
{
    return false;
}


int CEspScrubbedXmlService::onSimpleDataRequest(IEspContext &context, IEspSimpleDataRequest &req, IEspSimpleDataResponse &resp)
{
    const char *path = req.getPath();
    const char *method_name = req.getService();
    const char *xmlreq = req.getScrubbedXML();

    DBGLOG("Scrubbed XML:\n%s\n", xmlreq);

    resp.setMessageXML(xmlreq);
    resp.setResultsXML("<Dataset><Row><Apples>111</Apples><Oranges>333</Oranges></Row><Row><Apples>1</Apples><Oranges>12222</Oranges></Row></Dataset>");
    
    return 0;
}


int CEspScrubbedXmlService::onSimpleDataByRefRequest(IEspContext &context, IEspSimpleDataRequest & req, IEspSimpleDataRefResponse & resp)
{
    const char *path = req.getPath();
    const char *method_name = req.getService();
    const char *xmlreq = req.getScrubbedXML();

    resp.setResultsPath("wuid-10101010");
    resp.setMessageXML(xmlreq);
    
    return 0;
}


int CEspScrubbedXmlService::onSimpleDataRetrieval(IEspContext &context, IEspSimpleDataRetrievalRequest & req, IEspSimpleDataResponse & resp)
{
    const char *wuid_path = req.getResultsPath();

    resp.setResultsXML("<Dataset><Row><Pears>111</Pears><Bananas>333</Bananas></Row><Row><Pears>1</Pears><Bananas>12222</Bananas></Row></Dataset>");

    return 0;
}
