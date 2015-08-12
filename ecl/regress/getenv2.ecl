/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    This program is free software: you can redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#workunit ('name', 'aaaLibrary2'); // NOTE - not yet supported in Roxie but required in hthor.
#option ('targetClusterType', 'roxie');
import std.system.thorlib;

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

FilterDatasetInterface(dataset(namesRecord) ds, string search, boolean onlyOldies) := interface
    export dataset(namesRecord) matches;
    export dataset(namesRecord) others;
end;


filterDatasetLibrary(dataset(namesRecord) ds, string search, boolean onlyOldies) := module,library(FilterDatasetInterface)
    f := ds;
    shared g := if (onlyOldies, f(age >= 65), f);
    export matches := g(surname = search);
    export others := g((surname != search) and (surname != getEnv('SkipSurname', 'NoSkip')));
end;

#option ('targetClusterType', 'roxie');
build(filterDatasetLibrary);