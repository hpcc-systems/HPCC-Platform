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


idRecord := RECORD
    UNSIGNED f2;
END;

parentRecord := RECORD
   UNSIGNED f1;
   DATASET(idRecord) children;
END;

ds1 := DATASET([
    {1, [1,2,3] },
    {2, [2,3,4] },
    {3, [3] }], parentRecord);
    
ds2 := DATASET([
    {1, [5, 3, 8]},
    {2, [1,6] },
    {3, []}], parentRecord);
    
j1 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(JOIN(LEFT.children, RIGHT.children, LEFT.f2 = RIGHT.f2)));

jchild(parentRecord l, parentRecord r) := JOIN(l.children, r.children, LEFT.f2 = RIGHT.f2);
j2 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(jchild(LEFT, RIGHT)));


//LEFT is not valid as the second argument to the join until the right argument has been processed!
j3 := JOIN(ds1, ds2, LEFT.f1 = RIGHT.f1 AND EXISTS(JOIN(LEFT.children, LEFT.children(f2 > 10), LEFT.f2 = RIGHT.f2)));

sequential(
    output(j1);
    output(j2);
    output(j3);
);
