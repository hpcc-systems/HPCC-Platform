<Archive>
<!--

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    /*--HTML--
    &lt;DemoTag name="HTML"/&gt;
    */
    /*--RESULT--
    &lt;DemoTag name="RESULT"/&gt;
    */
    /*--HTMLD--
    &lt;DemoTag name="HTMLD"/&gt;
    */
    /*--SOAP--
    &lt;DemoTag name="SOAP"/&gt;
    */
    /*--HELP--
    ArbitraryHelpText.Example
    */
    /*--ERROR--
    &lt;DemoTag name="ERROR"/&gt;
    */
    /*--OTX--
    &lt;DemoTag name="OTX"/&gt;
    */
    /*--INFO-- Returns Information about the service. */
    //Following not currently extracted
    /*--INDEX--
    &lt;DemoTag name="INDEX"/&gt;
    */
    /*--FORM--
    &lt;DemoTag name="FORM"/&gt;
    */
    export attr1() := output('Done');
  </Attribute>
    <Attribute name="attr2">
export attr2 := 1;
    </Attribute>
  </Module>
  <Query attributePath="MODULE1.ATTR1"/>
</Archive>
