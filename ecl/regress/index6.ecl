/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

LayoutName := record
big_endian unsigned2    title;
string20        forename;
string20        surname;
              end;


LayoutNamePlus := record
LayoutName;
big_endian unsigned2        flags;
                ifblock(self.flags & 128 = 1)
string20            middle;
                end;
              end;


LayoutContact0 :=
            record
unsigned        id;
big_endian unsigned2        flags;
integer6        value;
LayoutName      master;
LayoutName      slave;
            end;

LayoutContact1 :=
            record
unsigned        id;
big_endian unsigned2        flags;
                ifblock(self.flags & 128 = 1)
integer6            value;
                end;
LayoutName      master;
//LayoutName        slave;
            end;

//-----------
ds0 := dataset('ds0', LayoutContact0, thor);


i0 := index(ds0, { id, }, { flags,  ifblock(self.flags & 128 = 1) value; end; }, 'i0');
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

output(__nameof__(i0));
output(__nameof__(ds0));
