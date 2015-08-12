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

string Mac_Append_files(string fullfile) :=
function
output(fullfile);
return 'good';
end;

// Record set
list_rec := record
            string name1;
end;

// Dataset out of file
d1 := dataset('~thor::temp::util_file_archive',list_rec,thor);

// new record set
out_rec := record
            string status;
end;

// Project transform to call the function
out_rec proj_rec(d1 t) := transform
            self.status := Mac_Append_files(t.name1);
end;


proj_out := project(d1,proj_rec(left));

output(proj_out);

