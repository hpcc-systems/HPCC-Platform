<Archive useArchivePlugins="1">
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
 <Module name="common">
  <Attribute name="m1">
export m1(unsigned k) := module,forward
export unsigned level1 := k*10;
export unsigned level2 := common.m2.level1 + level1;
export unsigned level3(unsigned i) := common.m2.level3(i) / level1;
    end;
  </Attribute>
  <Attribute name="m2">
export m2 := module,forward
export unsigned level1 := 10;
export unsigned level2 := common.m1(9).level1 * level1;
export unsigned level3(unsigned i) := i * (common.m1(6).level2 +123);
//missing end   end;
  </Attribute>
 </Module>
 <Query>
    import common;
 m := common.m1(15);
 output(m.level1);
 output(m.level2);
 output(m.level3(99));
 </Query>
</Archive>
