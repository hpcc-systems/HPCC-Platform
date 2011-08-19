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

    <xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="html"/>   
    <xsl:template match="/NotificationQueryResponse">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
        <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
            <title>Notification</title>
        </head>
        <body bgcolor="#e6e7de">
            <div align="left" class="unifont1">
            <blockquote>
            <xsl:apply-templates select="Subscription"/>
            <xsl:text>Alert:</xsl:text><br/>
            </blockquote>
            <blockquote>
            <blockquote>
            <xsl:apply-templates select="Notifications"/>
            </blockquote>
            </blockquote>
            </div>
        </body>
        </html>
    </xsl:template>

    <xsl:template match="Subscription">
        <xsl:if test="string-length(Note) > 0">
            <xsl:value-of select="Note"/>
            <p/>
        </xsl:if>
        <xsl:text>You've received this alert because you have subscribed for </xsl:text><xsl:value-of select="ServiceName"/> <xsl:text> alert:</xsl:text><br/> 
        <blockquote>
            <xsl:text>Subscription: </xsl:text><b><xsl:value-of select="SubscriptionName"/></b><br/>
            <xsl:if test="string-length(SearchRequest/SearchBy/Name/Last) > 0">
                <xsl:text>Last Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/Last"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Name/First) > 0">
                <xsl:text>First Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/First"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Name/Middle) > 0">
                <xsl:text>Middle Name: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Name/Middle"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/SSN) > 0">
                <xsl:text>SSN: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/SSN"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/DOB/Month) > 0">
                <xsl:text>DOB Month: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/DOB/Month"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/DOB/Day) > 0">
                <xsl:text>DOB Day: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/DOB/Day"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/DOB/Year) > 0">
                <xsl:text>DOB Year: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/DOB/Year"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/StreetAddress1) > 0">
                <xsl:text>StreetAddress1: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/StreetAddress1"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/StreetAddress2) > 0">
                <xsl:text>StreetAddress2: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/StreetAddress2"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/City) > 0">
                <xsl:text>City: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/City"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/State) > 0">
                <xsl:text>State: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/State"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/Zip5) > 0">
                <xsl:text>Zip5: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/Zip5"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Address/Zip4) > 0">
                <xsl:text>Zip4: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Address/Zip4"/></b><br/>
            </xsl:if>
            <xsl:if test="string-length(SearchRequest/SearchBy/Radius) > 0">
                <xsl:text>Radius: </xsl:text><b><xsl:value-of select="SearchRequest/SearchBy/Radius"/></b><br/>
            </xsl:if>
        </blockquote>
    </xsl:template>
    <xsl:template match="Notifications">
        <xsl:apply-templates select="Notification"/>
    </xsl:template>
    <xsl:template match="Notification">
        <xsl:text>-----------------------------------------------------------------</xsl:text><br/>
        <xsl:text>NotificationID: </xsl:text><b><xsl:choose><xsl:when test="string-length(BrowsingURL) > 0"><a href="{BrowsingURL}"><xsl:value-of select="NotificationId"/></a></xsl:when><xsl:otherwise><xsl:value-of select="NotificationId"/></xsl:otherwise></xsl:choose></b><br/>
        <xsl:text>Run Date: </xsl:text><b><xsl:value-of select="DateCreated"/></b><xsl:text> (GMT)</xsl:text><br/>
        <xsl:text>Status: </xsl:text><b><xsl:value-of select="Status"/></b><br/>
        <xsl:text>Records:</xsl:text><br/>      
        <xsl:apply-templates select="Content"/>
        <xsl:text>******************************************************************</xsl:text><br/>
    </xsl:template>

    <xsl:template match="Content">
        <xsl:apply-templates select="Records"/>
    </xsl:template>
    <xsl:template match="Records">
        <xsl:apply-templates select="Record"/>
    </xsl:template>

    <xsl:template match="Record">
        <xsl:text>******************************************************************</xsl:text><br/>
        <xsl:text>Name: </xsl:text><b><xsl:value-of select="Name/First"/><xsl:text> </xsl:text><xsl:value-of select="Name/Middle"/><xsl:text> </xsl:text><xsl:value-of select="Name/Last"/></b><br/>
        <xsl:text>AKAs: </xsl:text><b><xsl:apply-templates select="AKAs/Name"/></b><br/>
        <xsl:text>Address: </xsl:text><b><xsl:value-of select="Address/StreetNumber"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetPrefix"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetName"/><xsl:text> </xsl:text><xsl:value-of select="Address/StreetSuffix"/><xsl:text>, </xsl:text><xsl:value-of select="Address/City"/><xsl:text>, </xsl:text><xsl:value-of select="Address/State"/><xsl:text> </xsl:text><xsl:value-of select="Address/Zip5"/><xsl:text>-</xsl:text><xsl:value-of select="Address/Zip4"/></b><br/>
        <xsl:text>SSN: </xsl:text><b><xsl:value-of select="substring(SSN,1,5)"/><xsl:text>XXXX</xsl:text></b><br/>
        <xsl:text>DOB: </xsl:text><b><xsl:value-of select="DOB/Month"/><xsl:text>/XX/</xsl:text><xsl:value-of select="DOB/Year"/></b><br/>
        <xsl:text>Date Last Seen: </xsl:text><b><xsl:value-of select="DateLastSeen/Month"/><xsl:text>/</xsl:text><xsl:value-of select="DateLastSeen/Year"/></b><br/>
    </xsl:template>

    <xsl:template match="AKAs/Name">
        <xsl:if test="position() > 1">
            <xsl:text>, </xsl:text>
        </xsl:if>
        <xsl:value-of select="First"/><xsl:text> </xsl:text><xsl:value-of select="Middle"/><xsl:text> </xsl:text><xsl:value-of select="Last"/>
    </xsl:template>

</xsl:stylesheet>
