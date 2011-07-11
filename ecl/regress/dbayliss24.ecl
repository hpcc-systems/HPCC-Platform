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

layout_STATE_cases := record
  string STATE;
end;
LayoutScoredFetch := record,maxlength(32000) // Nulls required for linkpaths that do not have field
  integer i := 0;
integer d;
  dataset(layout_STATE_cases) STATE_cases := dataset([],layout_STATE_cases);
end;

d1 := dataset([{1}],{ integer l;});

layoutscoredfetch into(d1 l) := transform
  self.d := l.l;
  end;

p := project(d1,into(left));

output(p);
