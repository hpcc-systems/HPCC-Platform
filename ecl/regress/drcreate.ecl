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

fixedRecord :=
        RECORD
string20            forename;
string20            surname;
string2             nl := '\r\n';
        END;

variableRecord :=
        RECORD
string              forename;
string              surname;
string2             nl := '\r\n';
        END;

fixedRecord var2Fixed(variableRecord l) :=
    TRANSFORM
        SELF := l;
    END;

variableRecord fixed2Var(fixedRecord l) :=
    TRANSFORM
        SELF.forename := TRIM(l.forename);
        SELF.surname := TRIM(l.surname);
        SELF := l;
    END;

d := PIPE('pipeRead 20000 20', fixedRecord);
output(d,,'dtfixed');


d2 := PROJECT(d, fixed2Var(LEFT));
output(d2,,'dtvar');
