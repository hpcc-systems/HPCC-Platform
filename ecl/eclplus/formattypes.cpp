/*##############################################################################

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
############################################################################## */
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "formattypes.ipp"

#define HEX_COLUMNS     256     // only break records if > this value...
extern SCMStringBuffer resultName;

const char* formatxsl = 
"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
"<xsl:stylesheet version=\"1.0\" "
"   xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" "
"   xmlns:xs=\"http://www.w3.org/2001/XMLSchema\""
"   xmlns:xalan=\"http://xml.apache.org/xalan\""
"   exclude-result-prefixes=\"xalan xs\""
"   >"
"   <xsl:output method=\"text\" indent=\"yes\"/>"
"   <xsl:param name=\"attribute\"/>"
"   <xsl:param name=\"module\"/>"
"   <xsl:param name=\"dataDelimiter\"/>"
"   <xsl:param name=\"showHeader\" select=\"1\"/>"
"   <xsl:param name=\"showName\" select=\"0\"/>"
"   <xsl:param name=\"valueSeparator\"/>"
"   <xsl:param name=\"nameValueSeparator\"/>"
"   <xsl:param name=\"recordSeparator\"/>"
"   <xsl:param name=\"startRowNumber\" select=\"0\"/>"
"   <xsl:param name=\"showRecordNumber\" select=\"0\"/>"
""
"<xsl:template match=\"text()\">"
"</xsl:template>"
""
"<xsl:template match=\"Dataset\">"
"   <xsl:variable name=\"schema\" select=\"../XmlSchema[@name=current()/@xmlSchema]/*/xs:element[@name='Dataset']/*/*/xs:element[@name='Row']\"/>"
"   <xsl:if test=\"boolean($showHeader) and $startRowNumber = 0\">"
"   <xsl:call-template name=\"show-header\">"
"       <xsl:with-param name=\"schema\" select=\"$schema\"/>"
"   </xsl:call-template>"
"       <xsl:value-of select=\"$recordSeparator\"/>"
"   </xsl:if>"
"   <xsl:apply-templates select=\"Row\">"
"       <xsl:with-param name=\"dataset-schema\" select=\"$schema\"/>"
"   </xsl:apply-templates>"
"</xsl:template>"
""
"<xsl:template match=\"Row\">"
"   <xsl:param name=\"dataset-schema\"/>"
"   <xsl:if test=\"boolean($showRecordNumber)\">"
"       <xsl:text>[</xsl:text><xsl:value-of select=\"$startRowNumber+position()-1\"/><xsl:text>]</xsl:text>"
"       <xsl:value-of select=\"$recordSeparator\"/>"
"   </xsl:if>"
"   <xsl:call-template name=\"show-results\">"
"       <xsl:with-param name=\"schema\" select=\"$dataset-schema\"/>"
"       <xsl:with-param name=\"results\" select=\".\"/>"
"   </xsl:call-template>"
"   <xsl:value-of select=\"$recordSeparator\"/>"
"</xsl:template>"
""
"<xsl:template name=\"show-results\">"
"   <xsl:param name=\"schema\"/>"
"   <xsl:param name=\"results\"/>"
"   <xsl:for-each select=\"$schema/xs:complexType/xs:sequence/xs:element\">"
"       <xsl:variable name=\"name\" select=\"@name\"/>"
"       <xsl:choose>"
"           <xsl:when test=\"xs:complexType/xs:sequence[@maxOccurs='unbounded']\">"
"               <xsl:variable name=\"row-schema\" select=\"current()/*/*/xs:element[@name='Row']\"/>"
"               <xsl:if test=\"position() > 1\">"
"                   <xsl:value-of select=\"$valueSeparator\"/>"
"               </xsl:if>"
"               <xsl:if test=\"boolean($showName)\">"
"                   <xsl:value-of select=\"$row-schema/../../../@name\"/>"
"                   <xsl:value-of select=\"$nameValueSeparator\"/>"
"               </xsl:if>"
"               <xsl:value-of select=\"$dataDelimiter\"/>"
"               <xsl:variable name=\"data\" select=\"$results/*[name()=$name]\"/>"
"               <xsl:text>{</xsl:text>"
"               <xsl:for-each select=\"$data/Row\">"
"                   <xsl:call-template name=\"show-results\">"
"                       <xsl:with-param name=\"schema\" select=\"$row-schema\"/>"
"                       <xsl:with-param name=\"results\" select=\".\"/> "
"                   </xsl:call-template>"
"                   <xsl:value-of select=\"$recordSeparator\"/>"
"               </xsl:for-each>"
"               <xsl:text>}</xsl:text>"
"               <xsl:value-of select=\"$dataDelimiter\"/>"
"           </xsl:when>"
"           <xsl:when test=\"starts-with(@type, 'setof_')\">"
"               <xsl:variable name=\"colname\" select=\"@name\"/>"
"               <xsl:variable name=\"data\" select=\"$results/*[name()=$name]\"/>"
"               <xsl:if test=\"position() > 1\">"
"                   <xsl:value-of select=\"$valueSeparator\"/>"
"               </xsl:if>"
"               <xsl:if test=\"boolean($showName)\">"
"                   <xsl:value-of select=\"$colname\"/>"
"                   <xsl:value-of select=\"$nameValueSeparator\"/>"
"               </xsl:if>"
"               <xsl:text>[</xsl:text>"
"               <xsl:choose>"
"                   <xsl:when test=\"count($data/All)\">"
"                       <xsl:text>All</xsl:text>"
"                   </xsl:when>"
"                   <xsl:otherwise>"
"                       <xsl:for-each select=\"$data/Item\">"
"                           <xsl:value-of select=\"$dataDelimiter\"/>"
"                           <xsl:value-of select=\".\"/>"
"                       <xsl:value-of select=\"$dataDelimiter\"/>"
"                       <xsl:value-of select=\"$valueSeparator\"/>"
"                       </xsl:for-each>"
"                   </xsl:otherwise>"
"               </xsl:choose> "
"               <xsl:text>]</xsl:text>"
"           </xsl:when>"
"            <xsl:when test=\"xs:complexType\">"
"                <xsl:variable name=\"data\" select=\"$results/*[name()=$name]\"/>"
"                <xsl:call-template name=\"show-results\">"
"                    <xsl:with-param name=\"schema\" select=\"current()\"/>"
"                    <xsl:with-param name=\"results\" select=\"$data\"/> "
"                </xsl:call-template>"
"            </xsl:when>"
"            <xsl:when test=\"@name = 'Item' and @maxOccurs='unbounded'\">"
"               <xsl:call-template name=\"show-array\">"
"                   <xsl:with-param name=\"results\" select=\"$results\"/>"
"                   <xsl:with-param name=\"schema\" select=\"current()\"/>"
"               </xsl:call-template>"
"            </xsl:when>"
"            <xsl:otherwise>"
"               <xsl:call-template name=\"show-data\">"
"                   <xsl:with-param name=\"results\" select=\"$results\"/>"
"                   <xsl:with-param name=\"schema\" select=\"current()\"/>"
"                   <xsl:with-param name=\"name\" select=\"$name\"/>"
"                   <xsl:with-param name=\"pos\" select=\"position()\"/> "
"               </xsl:call-template>"
"            </xsl:otherwise>"
"        </xsl:choose> "
"   </xsl:for-each>"
"</xsl:template>"
""
"<xsl:template name=\"show-data\">"
"   <xsl:param name=\"results\"/>"
"   <xsl:param name=\"name\"/>"
"   <xsl:param name=\"schema\"/>"
"   <xsl:param name=\"pos\"/>"
"   <xsl:variable name=\"data\" select=\"$results/*[$pos]\"/>"
""
"       <xsl:if test=\"position() > 1\">"
"           <xsl:value-of select=\"$valueSeparator\"/>"
"       </xsl:if>"
"       <xsl:if test=\"boolean($showName)\">"
"           <xsl:value-of select=\"$schema/@name\"/>"
"           <xsl:value-of select=\"$nameValueSeparator\"/>"
"       </xsl:if>"
"       <xsl:value-of select=\"$dataDelimiter\"/>"
"       <xsl:choose>"
"           <xsl:when test=\"name($data)=$name\">"
"               <xsl:value-of select=\"$data\"/>"
"           </xsl:when>"
"           <xsl:otherwise>"
"               <xsl:value-of select=\"$results/*[name()=$name]\"/>"
"           </xsl:otherwise>"
"       </xsl:choose> "
"       <xsl:value-of select=\"$dataDelimiter\"/>"
"</xsl:template>"
""
"<xsl:template name=\"show-array\">"
"   <xsl:param name=\"results\"/>"
"   <xsl:param name=\"schema\"/>"
"   <xsl:text>[</xsl:text>"
"   <xsl:for-each select=\"$results/Item\">"
"       <xsl:if test=\"position() > 1\">"
"          <xsl:text>,</xsl:text>"
"       </xsl:if>"
"       <xsl:value-of select=\".\"/>"
"   </xsl:for-each>"
"   <xsl:text>]</xsl:text>"
"</xsl:template>"
""
"<xsl:template name=\"show-header\">"
"   <xsl:param name=\"schema\"/>"
"       <xsl:for-each select=\"$schema/xs:complexType/xs:sequence/xs:element\">"
"           <xsl:if test=\"position() > 1\">"
"               <xsl:value-of select=\"$valueSeparator\"/>"
"           </xsl:if>"
"           <xsl:value-of select=\"$dataDelimiter\"/>"
"           <xsl:value-of select=\"@name\"/>"
"           <xsl:value-of select=\"$dataDelimiter\"/>"
"       </xsl:for-each>"
"<xsl:text>"
"</xsl:text>"
"</xsl:template>"
""
"</xsl:stylesheet>";

void TextFormatType::printBody(FILE* fp, int len, char* txt)
{
    if(len <= 0 || txt == NULL)
        return;

    Owned<IXslTransform> transform = xslprocessor->createXslTransform();

    //fprintf(fp, "%s\n", txt);
    StringBuffer xmlbuf;
    xmlbuf.append("<Result>").append(len, txt).append("</Result>");
    transform->setXmlSource(xmlbuf.str(), xmlbuf.length());
    transform->setXslNoCache(formatxsl, strlen(formatxsl));
    transform->setParameter("showHeader", displayNamesHeader()?"1":"0");
    transform->setParameter("showName", embedNames()?"1":"0");
    transform->setParameter("showRecordNumber", displayRecordNumber()?"1":"0");
    StringBuffer valbuf;
    transform->setParameter("dataDelimiter", valbuf.clear().append("'").append(getDataDelimiter()).append("'").str());
    transform->setParameter("valueSeparator", valbuf.clear().append("'").append(getValueSeparator()).append("'").str());
    transform->setParameter("nameValueSeparator", valbuf.clear().append("'").append(getNameValueSeparator()).append("'").str());
    transform->setParameter("recordSeparator", valbuf.clear().append("'").append(getRecordSeparator()).append("'").str());
    transform->setParameter("startRowNumber", valbuf.clear().append(getStartRowNumber()).str());
    StringBuffer buf;
    
    transform->transform(buf);
    fprintf(fp, "%s", buf.str());
}

void TextFormatType::printHeader(FILE* fp, const char* name)
{
    if(displayQueryNumber())
    {
        fprintf(fp, "[%s]\n", name);
    }
}

void TextFormatType::printFooter(FILE* fp)
{
}

void XmlFormatType::printBody(FILE* fp, int len, char* txt)
{
    if(len <= 0 || txt == NULL)
        return;

    const char* bptr = strstr(txt, "<Dataset ");
    if(!bptr)
        return;
    bptr++;
    while(*bptr != '\0' && *bptr != '<')
        bptr++;
    char* eptr = (char*)(txt + len - 1);
    while(eptr > bptr && *eptr != '<')
        eptr--;
    if(eptr == bptr)
        return;
    *eptr = 0;
    fprintf(fp, " ");
    fprintf(fp, "%s", bptr);
}

void XmlFormatType::printHeader(FILE* fp, const char* name)
{
    fprintf(fp, "<Dataset name='%s'>\n", name);
}

void XmlFormatType::printFooter(FILE* fp)
{
    fprintf(fp, "</Dataset>\n");
}

void BinFormatType::printBody(FILE* fp, int len, char* txt)
{
    if(len <= 0 || txt == NULL)
        return;
    fwrite(txt, len, 1, fp);
}

void BinFormatType::printHeader(FILE* fp, const char* name)
{
}

void BinFormatType::printFooter(FILE* fp)
{
}

