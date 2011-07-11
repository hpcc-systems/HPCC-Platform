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
