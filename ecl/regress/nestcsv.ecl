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

//Convoluted test to check nested class in remote child is accessing the correct cursors.
#option ('targetClusterType', 'thor');

ds := dataset('ds', { string name, string sep }, thor);

linerec := { string line; };

outrec := record
dataset(linerec) lines;
        end;


outrec t(ds l) := transform

    self.lines := dataset(l.name, linerec, csv(separator(l.sep)));
    end;

output(project(ds, t(left)));
