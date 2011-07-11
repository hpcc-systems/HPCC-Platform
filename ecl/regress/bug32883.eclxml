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
  <Attribute name="attribute_a">
export attribute_A := MODULE
  export layout := RECORD
    unsigned did;
    attribute_B.counts;
  end;

  export ds := dataset ([], layout);
END;
  </Attribute>
  <Attribute name="attribute_b">
    import common;
export Attribute_B := MODULE
  export counts := RECORD
    unsigned attribute_a := 0;
    unsigned other := 0;
  end;
END;  </Attribute>
  <Attribute name="x">
export x := 100;
  </Attribute>
 </Module>
 <Query>
    import common;
    r := common.attribute_b;
 output(common.attribute_a.ds);
 </Query>
</Archive>
