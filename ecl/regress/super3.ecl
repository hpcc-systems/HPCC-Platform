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

// SEQUENTIALs is most probably redandant here, but doesn't harm it anyway

string grandsuper_name := '~thor::vmyullyari::temp::test_grandsuper';
string super_name      := '~thor::vmyullyari::temp::test_super';
string child_name      := '~thor::vmyullyari::temp::test';


// ------------------ create sample files, if not done before ------------------
ds_in := DATASET ([{'aaa'}], {string3 val});

createChain := SEQUENTIAL (
  FileServices.CreateSuperFile (grandsuper_name),
  FileServices.CreateSuperFile (super_name),
  OUTPUT (ds_in, , child_name),
  FileServices.AddSuperFIle (super_name, child_name),
  FileServices.AddSuperFIle (grandsuper_name, super_name)
);


ds_super := FileServices.LogicalFileSuperOwners (super_name);
super_list := SET (ds_super, name);

ds_child := FileServices.LogicalFileSuperOwners (child_name);
child_list := SET (ds_child, name);

// ------------------ test superfile owners ------------------
testChain := SEQUENTIAL (
  OUTPUT (super_list, NAMED ('superf_list')),
  OUTPUT (super_list[1] = '', NAMED ('super_1')), // should return FALSE
  OUTPUT (super_list[1]),

  OUTPUT (child_list, NAMED ('child_list')),
  OUTPUT (child_list[1] = '', NAMED ('child_1')), // should return FALSE
  OUTPUT (child_list[1])
);

// ------------------ RUN ------------------
SEQUENTIAL (
  createChain,
  testChain
);
