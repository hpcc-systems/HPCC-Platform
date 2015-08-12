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

#option ('foldAssign', false);
#option ('globalFold', false);

namesRecord :=
            RECORD
string20        surname;
bitfield10      age;
bitfield1       isOkay;
bitfield2       dead;       // y/N/maybe
varstring1          okay;
bitfield28      largenum;
bitfield1       extra;
bitfield63      gigantic;
bitfield1       isflag;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesRecord t(namesRecord l, namesRecord r) :=
        TRANSFORM
            SELF := r;
        END;

z := iterate(namesTable, t(LEFT,RIGHT));

output(z, {surname,age,isOkay,okay,if(okay != '' and okay != '',true,false),largenum,extra,gigantic,isflag,s:=surname},'out.d00');
