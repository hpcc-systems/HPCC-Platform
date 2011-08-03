/*##############################################################################

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
