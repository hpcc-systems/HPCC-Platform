<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:sa="urn:oasis:names:tc:SAML:2.0:assertion" xmlns:sp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:xa="urn:xform_assertion">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
    <xsl:template match="/">
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:LNSSO">
            <soap:Body>
                    <CreateSSOSessionRequest>
                        <ClientIP><xsl:value-of select="/xa:XForm_Assertion/xa:Context/xa:ClientIP"/></ClientIP>
                        <UserName><xsl:value-of select="/xa:XForm_Assertion/sp:Response/sa:Assertion/sa:Subject/sa:NameID"/></UserName>
                    </CreateSSOSessionRequest>
            </soap:Body>
        </soap:Envelope>
    </xsl:template>
</xsl:stylesheet>
