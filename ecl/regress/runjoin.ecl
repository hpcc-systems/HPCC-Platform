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
       integer1 v;
     end;

l := dataset([{1},{2},{3}], rec);
r := dataset([{1},{3},{4}], rec);

rec t(rec ll, rec rr) := TRANSFORM
    SELF := ll;
  END;

j1 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), LEFT ONLY);
output(j1);
j2 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), LEFT OUTER);
output(j2);
j3 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), RIGHT ONLY);
output(j3);
j4 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), RIGHT OUTER);
output(j4);
j5 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT));
output(j5);
j6 := JOIN(l, r, LEFT.v = RIGHT.v, t(LEFT, RIGHT), FULL OUTER);
output(j6);
