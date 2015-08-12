<?xml version="1.0" encoding="utf-8"?>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema">
<xsl:output method="text"/>

   <!--the methods SetDhcpInfo and MakeDhcp are not intended to generate any output unless 
       there are any exceptions, in which case only the exception message is desired-->       
<xsl:template match="//Exceptions/Exception/Message">
<xsl:value-of select="."/><xsl:text>
</xsl:text>
</xsl:template>
   
<xsl:template match="text()"/>
</xsl:stylesheet>
