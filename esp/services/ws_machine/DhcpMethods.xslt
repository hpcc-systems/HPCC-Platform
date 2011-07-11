<?xml version="1.0" encoding="utf-8"?>
<!--

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xalan="http://xml.apache.org/xalan">
<xsl:output method="text"/>

   <!--the methods SetDhcpInfo and MakeDhcp are not intended to generate any output unless 
       there are any exceptions, in which case only the exception message is desired-->       
<xsl:template match="//Exceptions/Exception/Message">
<xsl:value-of select="."/><xsl:text>
</xsl:text>
</xsl:template>
   
<xsl:template match="text()"/>
</xsl:stylesheet>
