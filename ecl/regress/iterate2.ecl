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

import std.system.thorlib;

export MAC_Sequence_Records_StrField(infile,idfield,outfile) := macro

//-- Transform that assigns id field
//   Assigns idfield according to node.
//   Sequential records on a node will have id fields that differ by the total number of nodes.
#uniquename(tra)
typeof(infile) %tra%(infile lef,infile ref) := transform
  self.idfield := (string)if(lef.idfield='',thorlib.node()+1,(integer)lef.idfield+400); // more use nodes()
  self := ref;
  end;

//****** Push infile through transform above
outfile := iterate(infile,%tra%(left,right),local);
  endmacro;



namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);



x := table(namesTable, {surname,forename,age,string4 countField := 0;});

MAC_Sequence_Records_StrField(x, countField, z)
//MAC_Sequence_Records_StrField(namesTable, surname, z)

output(z);
