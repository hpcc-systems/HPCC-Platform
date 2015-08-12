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
unsigned6       did;
string20        surname;
string10        forename;
integer2        age := 25;
string100       extra;
            END;

slimRecord :=
            RECORD
unsigned6       did;
string20        surname;
string10        forename;
            END;


processNameLibrary(dataset(namesRecord) ds) := interface
    export dataset(slimRecord) slim;
    export dataset(namesRecord) matches;
    export dataset(namesRecord) mismatches;
end;



//------ Library implementation -------------

impProcessNameLibrary(dataset(namesRecord) ds) := module,library(ProcessNameLibrary)
    shared ageIndex := index({ unsigned6 did }, { integer2 age }, 'AgeIndex');
    shared _slim := table(ds, { did, surname, forename });          // deliberatly a table based on parameter, rather than a project
    export slim := project(_slim, slimRecord);
    sortSlim := sort(_slim, surname, forename);
    dedupSlim := dedup(sortSlim, surname, forename, local);
    shared cleaned := join(dedupSlim, ageIndex, left.did = right.did, transform(namesRecord, self := left; self := right; self := []));
    export matches := join(ds, cleaned, left.did = right.did, transform(left), inner, keep(1));
    export mismatches := join(ds, cleaned, left.did = right.did, transform(left), left only);
end;


#workunit('name','ProcessNameLibrary');
BUILD(impProcessNameLibrary);
