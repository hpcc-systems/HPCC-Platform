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

layout := {
   unicode u1;
};

ds := dataset([{u'a'},{u''},{u'b'}], layout);

layout skipEmpty(layout rec) := transform

   // Error: Incompatible types: expected unicode, given
   // self.u1 := if(rec.u1 != u'', rec.u1, SKIP);

   // works correctly
   self.u1 := if(rec.u1 != u'', rec.u1, SKIP);

end;

ds2 := project(ds, skipEmpty(left));

output(ds2);
