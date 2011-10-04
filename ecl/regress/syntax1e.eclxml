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
 <Module name="ghalliday.nested">
  <Attribute name="gh1">
export gh1 := gh2 * gh3;
  </Attribute>
  <Attribute name="gh2">
export gh2 := 100;
  </Attribute>
  <Attribute name="gh3">
export gh3 := 200;
  </Attribute>
 </Module>
 <SyntaxCheck module="ghalliday.nested" attribute="gh1">
//Check no need to import, definition is overridden, and error reported
export gh1 := $.gh2 * 9;
 </SyntaxCheck>>
</Archive>
