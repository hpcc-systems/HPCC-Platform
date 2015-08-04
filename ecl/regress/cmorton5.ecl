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


export namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

export processFile(dataset(namesRecord) dsIn) := function

dsOut := dsIn(surname <> '');

failed := exists(dsIn) and ~ exists(dsOut);

if (failed, fail('Oh dear!' + (string)count(dsIn)));

return dsOut;
end;


namesTable := dataset('x',namesRecord,FLAT);

x := processFile(namesTable);


namesRecord t(x l) := transform

    SELF := l;
    end;


outRec := record
unsigned3 id;
        end;

d2 := dataset([{(unsigned3) count(x)}], outRec);

output(d2);
