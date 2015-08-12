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



idRecord := { unsigned id };

namesRecord :=
            RECORD
string20        name;
dataset(idRecord)   x{maxcount(100)};
dataset(idRecord)   y{maxcount(100)};
            END;

namesTable := nofold(dataset([
        {'Gavin', [1,2,3,4], [6,7,8,9]},
        {'John', [1,2], [6,7]},
        {'Jim', [1,2,3], [6,7]},
        {'Jimmy', [1,2,3,4,5,6,7,8], [6,7]}], namesRecord));


p := project(namesTable, transform(namesRecord, self.x := choosen(left.x+left.y, 5); self.y := []; self := left));

output(p);




id2Record := { string id{maxlength(100)} };

names2Record :=
            RECORD
string20        name;
dataset(id2Record)  x{maxcount(100)};
dataset(id2Record)  y{maxcount(100)};
            END;

names2Table := nofold(dataset([
        {'Gavin', ['1','2bcd','3','4'], ['6','7','8','9']},
        {'John', ['1','2bcd'], ['6','7']},
        {'Jim', ['1','2bcd','3'], ['6','7']},
        {'Jimmy', ['1','2bcd','3','4','5','6','7','8'], ['6','7']}], names2Record));


p2 := project(names2Table, transform(names2Record, self.x := choosen(left.x+left.y, 5); self.y := []; self := left));

output(p2);
