<Archive>
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
 <Module name="MoDuLe1">
  <Attribute name="AtTr1">
    //
    // Follow USES:
    // Tags: "HTML", "RESULT"
    // Other "INDEX", "FORM"
    // Don't follow USES:
    // Tags: "HTMLD", "SOAP", "INFO", "HELP", "ERROR", "OTX"
    // Other: DFORM
    // Take into account (1) USES chain for some tags and (2) default name
    // ?xgml tags must be stripped - even if multiple
   /*--SOAP--
&lt;message&gt;MySoapMessage&lt;/message&gt;
*/
/*--INFO-- Returns DID based on VRU input. */
/*--USES-- Module1.attr2 */
export attr1() := output('Done');
    </Attribute>
    <Attribute name="attr2">
/*--HTML--
&lt;?xml version="1.0" encoding="utf-8"?&gt;
&lt;xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"&gt;
  &lt;xsl:import href="./esp/xslt/lib.xslt"/&gt;
  &lt;xsl:param name="url" select="'unknown'"/&gt;
  &lt;xsl:template match="text()"/&gt;
&lt;/xsl:stylesheet&gt;
*/

/*--HTMLD--
&lt;?xml version="1.0" encoding="utf-8"?&gt;
&lt;xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"&gt;
&lt;/xsl:stylesheet&gt;
*/

/*--INDEX--
&lt;?xml version="1.0" encoding="utf-8"?&gt;
&lt;xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"&gt;
&lt;/xsl:stylesheet&gt;
*/

/*--RESULT--
&lt;?xml version="1.0" encoding="utf-8"?&gt;
&lt;xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"&gt;
&lt;/xsl:stylesheet&gt;
*/
/*--ERROR--
&lt;?xml version="1.0" encoding="utf-8"?&gt;
&lt;xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"&gt;
&lt;/xsl:stylesheet&gt;
*/
export attr2 := 1;
    </Attribute>
  </Module>
 <Query attributePath="MODULE1.ATTR1"/>
</Archive>
