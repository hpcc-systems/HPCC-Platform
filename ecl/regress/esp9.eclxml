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
    /*--USES-- Module1.attr2 */
    /*--HTML-- Module1.attr3 */
    export attr1() := output('Done');
  </Attribute>
    <Attribute name="attr2">
    /*--USES-- Module1.attr3 */
    /*--RESULT--
    &lt;DemoTag name="RESULT2"/&gt;
    */
    /*--SOAP--
    &lt;DemoTag name="SOAP2"/&gt;
    */
    /*--INFO-- Returns Information about the service. */
export attr2 := 1;
    </Attribute>
   <Attribute name="attr3">
     /*--USES-- Module1.attr4 */
     /*--HTML--
     &lt;DemoTag name="HTML3"/&gt;
     */
     /*--HTMLD--
     &lt;DemoTag name="HTMLD3"/&gt;
     */
     /*--SOAP--
     &lt;DemoTag name="SOAP3"/&gt;
     */
     /*--OTX--
     &lt;DemoTag name="OTX3"/&gt;
     */
     export attr3 := 1;
   </Attribute>
   <Attribute name="attr4">
     /*--HELP--
     ArbitraryHelpText.Example4
     */
     /*--ERROR--
     &lt;DemoTag name="ERROR4"/&gt;
     */
     export attr4 := 1;
   </Attribute>
 </Module>
  <Query attributePath="MODULE1.ATTR1"/>
</Archive>
