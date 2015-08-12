<Archive useArchivePlugins="1">
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
