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
