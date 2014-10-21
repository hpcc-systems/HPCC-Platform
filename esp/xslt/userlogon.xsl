<?xml version="1.0" encoding="UTF-8"?>
<!--

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
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fo="http://www.w3.org/1999/XSL/Format">
    <xsl:output method="html"/>
    <xsl:template match="/">
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en">
            <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>User Log On</title>
                <script type="text/javascript" language="javascript">
                    <xsl:text disable-output-escaping="yes"><![CDATA[
                        function checkfields(f)
                        {
                            if(f.username.value != '' && f.password.value != '')
                                f.LogOn.disabled=false;
                            else
                                f.LogOn.disabled=true;
                        }
                        function onLoad()
                        {
                            document.getElementById("username").focus();
                        }
                    ]]></xsl:text>
                </script>
            </head>
            <body class="yui-skin-sam" onload="onLoad()">
                <p align="left" />
                <xsl:apply-templates/>
            </body>
        </html>
    </xsl:template>
    
    <xsl:template match="UserLogOn">
        <h3>
            <xsl:choose>
                <xsl:when test="string-length(Message)">
                    <xsl:value-of select="Message"/>
                </xsl:when>
                <xsl:otherwise>
                    Please Log on.
                </xsl:otherwise>
            </xsl:choose>
            <br/>
        </h3>
        <form id="user_input_form" name="user_input_form" method="GET" action="/">
            <table>
                <tr>
                    <td>
                        <b>Username: </b>
                    </td>
                    <td>
                        <input type="text" id="username" name="username" size="20" onKeyPress="checkfields(this.form)"/>
                    </td>
                </tr>
                <tr>
                    <td>
                        <b>Password: </b>
                    </td>
                    <td>
                        <input type="password" name="password" size="20" value="" onKeyPress="checkfields(this.form)" onChange="checkfields(this.form)"/>
                    </td>
                </tr>
                <tr>
                    <td/>
                    <td>
                        <input type="submit" id="LogOn" name="LogOn" value="Log on" disabled="true"/>
                    </td>
                </tr>
            </table>
        </form>
    </xsl:template>
</xsl:stylesheet>
