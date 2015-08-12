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

import lib_fileservices;

string Mac_Append_Files(string basefile,string infile) :=
function
Fileservices.addsuperfile(basefile,infile);
return infile;
end;

list_rec := record
    string name1;
end;

d1 := dataset('~thor::temp::util_file_archive',list_rec,thor);

out_rec := record
    string status := '';
end;


nothor(apply(global(d1,few),Fileservices.addsuperfile('~thor::base::utility_file_test',name1)));

/*out_rec proj_rec(d1 t) := transform
    self.status := Mac_Append_Files('~thor::base::utility_file_test','~'+t.name1);
end;

proj_out := project(d1,proj_rec(left));



output(proj_out);*/

