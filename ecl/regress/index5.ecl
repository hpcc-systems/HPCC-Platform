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


LayoutName := record
integer2        title;
string20        forename;
string20        surname;
              end;


LayoutNamePlus := record
LayoutName;
integer2        flags;
                ifblock(self.flags & 128 = 1)
string20            middle;
                end;
              end;


LayoutContact0 := 
            record
unsigned        id;
integer2        flags;
integer6        value;
LayoutName      master;
LayoutName      slave;
            end;

LayoutContact1 := 
            record
unsigned        id;
integer2        flags;
                ifblock(self.flags & 128 = 1)
integer6            value;
                end;
LayoutName      master;
//LayoutName        slave;
            end;

//-----------
ds0 := dataset('ds0', LayoutContact0, thor);


i0 := index(ds0, { id, }, { flags,  ifblock(self.flags & 128 != 0) ifblock(self.flags & 64 != 0) value; end; end; }, 'i0');
buildindex(i0);
output(i0(id=10));
output(i0(id=10 and value < 1000));

i01 := index(ds0, { id, }, { flags,  ifblock(self.flags & 128 = 1) ds0.value; end; ds0 }, 'i01');
buildindex(i01);
output(i01(id=10));
output(i01(id=10 and value < 1000));

//------------

ds1 := dataset('ds1', LayoutContact1, thor);
i1 := index(ds1, { id, flags }, { ds1 }, 'i1');
buildindex(i1);             
output(i1(id=10));// and value < 1000));
output(i1(id=10 and value < 1000));
output(i1(id=10 and master.forename = 'Gavin'));
