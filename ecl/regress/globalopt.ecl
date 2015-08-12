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


namesRecord :=
            RECORD
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string20        surname;
dataset(namesRecord) names;
            END;

namesTable := dataset('nt', namesRecord, thor);
addrTable := dataset('nt', addressRecord, thor);



sortNames(dataset(namesRecord) inFile) := function
    g := global(inFile, opt, few);
    return g[1] + sort(g[2..], surname);
end;


output(sortNames(namesTable));

output(project(addrTable, transform(addressRecord, self.names := sortNames(left.names); self := left)));

