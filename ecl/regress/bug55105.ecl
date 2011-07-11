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
   integer i1,
   string  s2,
   unicode u3,
};

ds := dataset([{1,'a',u'z'},{2,'b',u'y'}], layout);

layout throwError(layout rec) := transform

   self.i1 := if(rec.i1 != 0,   rec.i1, error('message1'));
   self.s2 := if(rec.s2 != 'c', rec.s2, error('message2'));

   // Without the cast to unicode, syntax check will fail with 
   // "Error: Incompatible types: expected unicode, given "
   self.u3 := if(rec.u3 != u'x', rec.u3, (unicode) error('message3'));

end;

ds2 := project(ds, throwError(left));
output(ds2);
