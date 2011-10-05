<Archive testRemoteInterface="1">
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
    <Module name="">
        <Attribute name="a0">
            EXPORT a0 := MODULE
            export v1 := 'level0';
            END;
        </Attribute>
    </Module>
    <Module name="a">
        <Attribute name="a1">
            EXPORT a1 := 'level1';
        </Attribute>
    </Module>
    <Module name="b.c">
        <Attribute name="a2">
            EXPORT a2 := 'level2';
        </Attribute>
    </Module>
    <Module name="d.e.f">
        <Attribute name="a3">
            EXPORT a3 := 'level3';
        </Attribute>
    </Module>
    <Query>
        import a0,a,b.c,d.e.f;
        output(a0.v1);
        output(a.a1);
        output(c.a2);
        output(f.a3);
    </Query>
</Archive>
