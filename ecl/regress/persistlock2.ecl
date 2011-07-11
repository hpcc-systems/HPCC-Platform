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

rtl := service
unsigned4 delayReturn(unsigned4 value, unsigned4 sleepTime) : eclrtl,entrypoint='rtlDelayReturn';
        end;

unsigned4 waitTime := 1000;

p1 := 1 : persist('p1');

p2 := rtl.delayReturn(p1,waitTime) : persist('p2');
p3 := rtl.delayReturn(p2,waitTime) : persist('p3');
p4 := rtl.delayReturn(p3,waitTime) : persist('p4');
p5 := rtl.delayReturn(p4,waitTime) : persist('p5');
p6 := rtl.delayReturn(p5,waitTime) : persist('p6');
p7 := rtl.delayReturn(p6,waitTime) : persist('p7');
p8 := rtl.delayReturn(p7,waitTime) : persist('p8');
p9 := rtl.delayReturn(p8,waitTime) : persist('p9');

output(p9);
