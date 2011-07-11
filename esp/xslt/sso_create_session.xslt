<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:sa="urn:oasis:names:tc:SAML:2.0:assertion" xmlns:sp="urn:oasis:names:tc:SAML:2.0:protocol" xmlns:xa="urn:xform_assertion">
    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes" omit-xml-declaration="yes"/>
    <xsl:template match="/">
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope" xmlns="urn:LNSSO">
            <soap:Body>
                    <CreateSSOSessionRequest>
                        <ClientIP><xsl:value-of select="/xa:XForm_Assertion/xa:Context/xa:ClientIP"/></ClientIP>
                        <UserName><xsl:value-of select="/xa:XForm_Assertion/sp:Response/sa:Assertion/sa:Subject/sa:NameID"/></UserName>
                    </CreateSSOSessionRequest>
            </soap:Body>
        </soap:Envelope>
    </xsl:template>
</xsl:stylesheet>
