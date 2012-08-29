<?xml version="1.0" encoding="UTF-8"?>
<!--
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

    <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <!--xsl:output method="html"/>   
    <xsl:template match="/NotificationQueryResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Notification</title>
            <script language="JavaScript1.2">
                var currentDate = '';
                function onLoad()
                {
                    currentDate = Date();
                    return;
                }
            </script>       
        </head>
        <body bgcolor="#e6e7de"  onload="onLoad()">
            <div align="center" class="unifont1">
            <script language = "JavaScript">
                var now = new Date();
                var timestring = now.toLocaleString();
                var startpos = timestring.indexOf(" ");
                document.write("Saved Search Update Report: " + timestring.substr(startpos + 1));
            </script>
            </div>
            <br/> 
            <div align="left" class="unifont1">
            <blockquote>
                <xsl:text>You've received this alert because you have subscribed for </xsl:text><b><xsl:value-of select="Subscription/ServiceName"/></b><xsl:text> alert:</xsl:text><br/> 
                <br/> 
                <blockquote>
                    <xsl:apply-templates select="Subscription"/>
                    <xsl:apply-templates select="Notification"/>
                </blockquote>
                <xsl:apply-templates select="Content"/>
            </blockquote>
            </div>
        </body>
        </html>
    </xsl:template-->
    <xsl:output method="text" encoding='iso-8859-1'/>   
    <xsl:template match="/NotificationQueryResponse">
        <xsl:text>
</xsl:text>
        <xsl:text>LexisNexis&#174;
        
</xsl:text>
        <xsl:apply-templates select="Subscription"/>
        <xsl:apply-templates select="Notification"/>
        <xsl:apply-templates select="Content"/>
    </xsl:template>

    <xsl:template match="Subscription">
        <xsl:if test="string-length(SubscriptionCreated) > 0">
            <xsl:value-of select="SubscriptionCreated"/><xsl:text> (GMT)

</xsl:text>
        </xsl:if>
        <xsl:text>-----------------------------------------------------------------------------------
        
</xsl:text>
        <xsl:text>Rollup Business Report Email Alert
        
</xsl:text>

        <xsl:if test="string-length(SubscriberID) > 0"> 
            <xsl:text>   Subscriber: </xsl:text><xsl:value-of select="SubscriberID"/>
<xsl:text>
</xsl:text>
        </xsl:if>

        <xsl:text>   Subscription: </xsl:text><b><xsl:value-of select="SubscriptionName"/></b><br/>
<xsl:text>
</xsl:text>
        <xsl:if test="string-length(Note) > 0"> 
            <xsl:text>   Note: </xsl:text><xsl:value-of select="Note"/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/BusinessId) > 0">
            <xsl:text>   Business Id: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/BusinessId"/><xsl:text>; </xsl:text></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
    </xsl:template>

    <xsl:template match="Notification">
            <xsl:text>   Date Created: </xsl:text><b><xsl:value-of select="DateCreated"/><xsl:text> (GMT)</xsl:text></b>
<xsl:text>
</xsl:text>
<xsl:text>
</xsl:text>
    </xsl:template>

    <xsl:template match="Content">
        <xsl:text>**************************************************************************
        
</xsl:text>
        <xsl:text>   Report For: </xsl:text><b><xsl:value-of select="ReportFor/CompanyName"/></b><br/>
<xsl:text>
</xsl:text>
        <xsl:text>   Address: </xsl:text><b><xsl:value-of select="ReportFor/Address/StreetNumber"/><xsl:text> </xsl:text><xsl:value-of select="ReportFor/Address/StreetPrefix"/><xsl:text> </xsl:text><xsl:value-of select="ReportFor/Address/StreetName"/><xsl:text> </xsl:text><xsl:value-of select="ReportFor/Address/StreetSuffix"/><xsl:text>, </xsl:text><xsl:value-of select="ReportFor/Address/City"/><xsl:text>, </xsl:text><xsl:value-of select="ReportFor/Address/State"/><xsl:text> </xsl:text><xsl:value-of select="ReportFor/Address/Zip5"/><xsl:text>-</xsl:text><xsl:value-of select="ReportFor/Address/Zip4"/></b><br/><br/>
<xsl:text>
</xsl:text>
        <xsl:text>   Name Variations: </xsl:text>
<xsl:text>
</xsl:text>
        <xsl:apply-templates select="NameVariations"/>
        <xsl:text>   Phone Variations: </xsl:text>
<xsl:text>
</xsl:text>
        <xsl:apply-templates select="PhoneVariations"/>
        <xsl:text>   Address Variations: </xsl:text>
<xsl:text>
</xsl:text>
        <xsl:apply-templates select="AddressVariations"/>
<xsl:text>
</xsl:text>
<xsl:text>-----------------------------------------------------------------------------------</xsl:text>
    </xsl:template>

    <xsl:template match="NameVariations">
        <xsl:apply-templates select="CompanyName"/>
    </xsl:template>

    <xsl:template match="CompanyName">
        <xsl:text>       Company Name: </xsl:text><b><xsl:value-of select="."/></b><br/>
<xsl:text>
</xsl:text>
    </xsl:template>

    <xsl:template match="PhoneVariations">
        <xsl:apply-templates select="PhoneInfo"/>
    </xsl:template>

    <xsl:template match="PhoneInfo">
        <xsl:text>       Phone Number: </xsl:text><b><xsl:value-of select="Phone10"/></b><br/>
<xsl:text>
</xsl:text>
    </xsl:template>

    <xsl:template match="AddressVariations">
        <xsl:apply-templates select="AddressInfo"/>
    </xsl:template>

    <xsl:template match="AddressInfo">
        <xsl:text>       Address: </xsl:text><b><xsl:value-of select="Address/StreetNumber"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetPrefix"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetName"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetSuffix"/><xsl:text>, </xsl:text><xsl:value-of select="Address/City"/><xsl:text>, </xsl:text><xsl:value-of select="Address/State"/><xsl:text> </xsl:text><xsl:value-of select="Address/Zip5"/></b>
<xsl:text>
</xsl:text>
        <xsl:if test="string-length(Address/Zip4) > 0">
            <xsl:text>-</xsl:text><xsl:value-of select="Address/Zip4"/>
<xsl:text>
</xsl:text>
        </xsl:if>
    </xsl:template>

</xsl:stylesheet>
