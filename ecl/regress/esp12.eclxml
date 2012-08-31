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
 <Module name="MoDuLe1.NeStEd">
  <Attribute name="AtTr1">
    /*--HTML-- Module1.nested.attr2 */
    /*--HTMLD-- Module1.nested.attr3 */
    /*--SOAP-- Module1.nested.attr2 */
    /*--HELP--
    ArbitraryHelpText.Example1
    */
    export attr1() := output('Done');
  </Attribute>
    <Attribute name="attr2">
    /*--HTML--
    &lt;DemoTag name="HTML2"/&gt;
    */
    /*--RESULT--
    &lt;DemoTag name="RESULT2"/&gt;
    */
    /*--HTMLD--
    &lt;DemoTag name="HTMLD2"/&gt;
    */
    /*--SOAP--
    &lt;DemoTag name="SOAP2"/&gt;
    */
    /*--HELP--
    ArbitraryHelpText.Example2
    */
    /*--ERROR--
    &lt;DemoTag name="ERROR2"/&gt;
    */
    /*--OTX--
    &lt;DemoTag name="OTX2"/&gt;
    */
    /*--INFO-- Returns Information about the service. */
    //Following not currently extracted
    /*--INDEX--
    &lt;DemoTag name="INDEX2"/&gt;
    */
    /*--FORM--
    &lt;DemoTag name="FORM2"/&gt;
    */
export attr2 := 1;
    </Attribute>
   <Attribute name="attr3">
     /*--HTML--
     &lt;DemoTag name="HTML3"/&gt;
     */
     /*--RESULT--
     &lt;DemoTag name="RESULT3"/&gt;
     */
     /*--HTMLD--
     &lt;DemoTag name="HTMLD3"/&gt;
     */
     /*--SOAP--
     &lt;DemoTag name="SOAP3"/&gt;
     */
     /*--HELP--
     ArbitraryHelpText.Example2
     */
     /*--ERROR--
     &lt;DemoTag name="ERROR3"/&gt;
     */
     /*--OTX--
     &lt;DemoTag name="OTX3"/&gt;
     */
     /*--INFO-- Returns Information about the service. */
     //Following not currently extracted
     /*--INDEX--
     &lt;DemoTag name="INDEX3"/&gt;
     */
     /*--FORM--
     &lt;DemoTag name="FORM3"/&gt;
     */
     export attr3 := 1;
   </Attribute>
 </Module>
   <Query attributePath="MODULE1.nested.ATTR1"/>
</Archive>
