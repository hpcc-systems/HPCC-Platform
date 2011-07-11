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
  </Module>
  <Query attributePath="MODULE1.ATTR1"/>
</Archive>
