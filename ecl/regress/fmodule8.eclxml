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
export m1 := module,forward
export unsigned level1 := SyntaxError;
//unsigned scale := level1;
export unsigned level2 := common.m2.levela * level1;
export unsigned level3(unsigned i) := common.m2.levelc(i) * level1;
    end;
  </Attribute>
  <Attribute name="m2">
export m2 := module,forward
export unsigned levela := 10;
export unsigned levelb := m1.level1 * levela;
export unsigned levelc(unsigned i) := i * (common.m1.level2 +123);
    end;
  </Attribute>
  <Attribute name="x">
export x := 100;
  </Attribute>
 </Module>
 <Query>
    import common;
 output(common.m1.level1);
 output(common.m1.level1);
 output(common.m1.level2);
 output(common.m1.level3(99));
 </Query>
</Archive>
