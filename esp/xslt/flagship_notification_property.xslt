<?xml version="1.0" encoding="UTF-8"?>
<!--

## Copyright © 2011 HPCC Systems.  All rights reserved.
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
        <xsl:text>Property Report Email Alert
        
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
        <xsl:if test="string-length(SearchRequest/SearchBy/CompanyName) > 0">
            <xsl:text>   Company Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/CompanyName"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/ParcelId) > 0">
            <xsl:text>   Parcel Id: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/ParcelId"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/FaresId) > 0">
            <xsl:text>   Fares Id: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/FaresId"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/SourcePropertyRecordId) > 0">
            <xsl:text>   Source Property Record Id: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/SourcePropertyRecordId"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/UniqueId) > 0">
            <xsl:text>   Unique Id: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/UniqueId"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Name/Last) > 0">
            <xsl:text>   Last Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/Last"/><xsl:text>; </xsl:text></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Name/First) > 0">
            <xsl:text>   First Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/First"/><xsl:text>; </xsl:text></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Name/Middle) > 0">
            <xsl:text>   Middle Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/Middle"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/StreetAddress1) > 0">
            <xsl:text>   Street Address 1: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/StreetAddress1"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/StreetAddress2) > 0">
            <xsl:text>   Street Address 2: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/StreetAddress2"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/City) > 0">
            <xsl:text>   City: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/City"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/State) > 0">
            <xsl:text>   State: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/State"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/Zip5) > 0">
            <xsl:text>   Zip: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/Zip5"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(SearchRequest/SearchBy/Address/Zip4) > 0">
            <xsl:text>   Zip4: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/Zip4"/></b><br/>
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
        <xsl:apply-templates select="Records"/>
<xsl:text>-----------------------------------------------------------------------------------</xsl:text>
    </xsl:template>

    <xsl:template match="Records">
        <xsl:apply-templates select="Record"/>
    </xsl:template>

    <xsl:template match="Record">
        <xsl:text>   Record Type: </xsl:text><b><xsl:value-of select="RecordType"/></b><br/>
<xsl:text>
</xsl:text>
        <xsl:text>   Record Type Description: </xsl:text><b><xsl:value-of select="RecordTypeDesc"/></b><br/>
<xsl:text>
</xsl:text>
        <!--xsl:text>   Fares Id: </xsl:text><b><xsl:value-of select="FaresId"/></b><br/>
<xsl:text>
</xsl:text-->
        <xsl:text>   Source Property Record Id: </xsl:text><b><xsl:value-of select="SourcePropertyRecordId"/></b><br/>
<xsl:text>
</xsl:text>
        <xsl:if test="string-length(Deed/County) > 0">
            <xsl:text>   County: </xsl:text><b><xsl:value-of select="Deed/County"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(Deed/ParcelId) > 0">
            <xsl:text>   Parcel Id: </xsl:text><b><xsl:value-of select="Deed/ParcelId"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
        <xsl:if test="string-length(Deed/SalesPrice) > 0">
            <xsl:text>   Sales Price: </xsl:text><b><xsl:value-of select="Deed/SalesPrice"/></b><br/>
<xsl:text>
</xsl:text>
        </xsl:if>
<xsl:text>
</xsl:text>
    </xsl:template>

</xsl:stylesheet>
