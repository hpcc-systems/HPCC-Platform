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
string123    id;
unsigned6    did;
integer6     zid1;
packed integer4 pid;
integer6     zid2;
        end;


unsigned6 didSeek := 0 : stored('didSeek');
integer4 pidSeek := 0 : stored('pidSeek');
integer6 zidSeek := 0 : stored('zidSeek');

rawfile := dataset('~thor::rawfile', rec, THOR, preload);
filtered := rawfile(keyed(did = didSeek));
output(filtered);

filtered2 := rawfile(keyed(zid1 = zidSeek));
output(filtered2);

filtered3 := rawfile(keyed(zid2 = zidSeek));
output(filtered3);

filtered4 := rawfile(keyed(pid = pidSeek));
output(filtered4);
