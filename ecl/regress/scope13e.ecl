/*##############################################################################

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
############################################################################## */

//I'm really not sure about this syntax for a scope resolution operator.....

export valueE := 1;
valueP := 2;

m := module

    export valueE := 10;

    integer valueP := 20;

    export m := module

        export valueE := 100;

        integer valueP := 200;

        export m := module

            export valueE := 1000;

            integer valueP := 2000;

            export m := module

                export valueE := 10000;

                integer valueP := 20000;

                export vP1 := valueP;
                export vE1 := valueE;
                export vP2 := ^.valueP;
                export vE2 := ^.valueE;
                export vP3 := ^^.valueP;
                export vE3 := ^^.valueE;
                export vP4 := ^^^.valueP;
                export vE4 := ^^^.valueE;
                export vP5 := ^^^^.valueP;
                export vE5 := ^^^^.valueE;
                export vP6 := ^^^^^.valueP;     // too many
                export vE6 := ^^^^^.valueE;
            end;
        end;
    end;
end;

output(m.m.m.m.vp1);
output(m.m.m.m.ve1);
output(m.m.m.m.vp2);
output(m.m.m.m.ve2);
output(m.m.m.m.vp3);
output(m.m.m.m.ve3);
output(m.m.m.m.vp4);
output(m.m.m.m.ve4);
output(m.m.m.m.vp5);
output(m.m.m.m.ve5);
