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

rec := record
string5 zip;
string1 other;
end;

ds:=dataset('xx',rec,flat);

rec tra(rec l, string5 zipx) := transform
  self.zip := zipx+'xx';
  self := l;
end;

//new_ds := project(ds, tra(left, left.zip));

new_ds := project(ds, tra(left, ds.zip));

output(new_ds);