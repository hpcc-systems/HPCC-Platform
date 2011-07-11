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
