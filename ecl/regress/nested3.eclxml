<Archive testRemoteInterface="0">
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
    <Attribute name="a0">
        EXPORT a0 := MODULE
        export v1 := 'level0';
        END;
    </Attribute>
    <Module name="a">
        <Attribute name="a1">
            EXPORT a1 := 'level1';
        </Attribute>
    </Module>
    <Module name="b">
        <Module name="c">
            <Attribute name="a2">
            EXPORT a2 := 'level2';
            </Attribute>
        </Module>
    </Module>
    <Module name="d">
        <Module name="e">
            <Module name="f">
                <Attribute name="a3">
                    EXPORT a3 := 'level3';
                </Attribute>
            </Module>
        </Module>
    </Module>
    <Query>
        import a0,a,b.c,d.e.f;
        output(a0.v1);
        output(a.a1);
        output(c.a2);
        output(f.a3);
    </Query>
</Archive>
