<Archive>
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
/*--INFO-- Returns DID based on VRU input. */
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
