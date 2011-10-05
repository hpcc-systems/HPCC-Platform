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
    <Module name="a.b">
        <Attribute name="c">
            export c := 1;
        </Attribute>
        <Attribute name="d">
            export d := 2;
        </Attribute>
    </Module>
    <Module name="a.x">
        <Attribute name="c">
            export c := 3;
        </Attribute>
        <Attribute name="d">
            export d := 4;
        </Attribute>
    </Module>
    <Module name="x">
        <Module name="y">
            <Attribute name="c">
                export c := 5;
            </Attribute>
            <Attribute name="d">
                export d := 6;
            </Attribute>
        </Module>
    </Module>
    <Attribute name="z">
        export z := MODULE
            export a := 7;
        END;
    </Attribute>
    <Query>
        //Test the different styles of nested modules now supported
        import a;
        output(a.b.c);
        output(a.b.d);
        output(a.x.c);
        output(a.x.d);
        import x.y;
        output(y.c);
        output(y.d);
        import z;
        output(z.a);
    </Query>
</Archive>
