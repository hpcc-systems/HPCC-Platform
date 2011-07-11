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
 <Module name="x1">
  <Attribute name="m1">
     import * from x2;
export m1 := x2.m1;
  </Attribute>
 </Module>
   <Module name="x2">
      <Attribute name="m1">
         export m1 := 123;
      </Attribute>
   </Module>
   <Module name="x3">
      <Attribute name="m1">
         export m1 := 456;
      </Attribute>
   </Module>
 <Query>
    import x1;
 output(x1.m1);
 </Query>
</Archive>
