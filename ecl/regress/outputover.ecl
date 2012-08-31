/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#option ('workflow', true);

ReportFail(string s) := output((string200)s, named('ErrorReason'),overwrite);

rec := record
string surname;
unsigned2 age;
        end;


ds := dataset('ds', rec, xml) : failure(reportFail('xml read'));

ds2 := ds(age != 100) : failure(reportFail('filter'));

output(ds2) : failure(reportFail('output'));
