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

