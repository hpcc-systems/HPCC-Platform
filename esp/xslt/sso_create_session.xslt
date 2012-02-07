<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
