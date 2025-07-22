/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#option ('generateLogicalGraph', true);

// An example of a library - which can have multiple inputs, multiple outputs
// and each of those outputs can be used multiple times.

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetLibrary(string search, dataset(namesRecord) ds, boolean onlyOldies) := interface
    export included := ds;
    export excluded := ds;
    export string name;
end;

namesTable := dataset('x',namesRecord,FLAT);

boolean isFCRA := false : stored('isFCRA');
NameFilterName := IF(isFCRA, 'NameFilter.FCRA', 'NameFilter');

filtered := LIBRARY(NameFilterName, FilterDatasetLibrary('Smith', namesTable, false));

smithIncluded := filtered.included;
smithExcluded := filtered.excluded;
output(smithIncluded(age < 18),,named('SmithMinor'));
output(smithIncluded(age >= 18),,named('SmithMajor'));
output(smithExcluded(age < 18),,named('NonSmithMinor'));
output(smithExcluded(age >= 18),,named('NonSmithMajor'));
