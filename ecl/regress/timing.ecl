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

rtl := SERVICE
unsigned4 msTick() : eclrtl,library='eclrtl',c,entrypoint='rtlTick';
END;

DoThing1() := output('hello');
DoThing2() := output('hello again');

unsigned4 StartTime := rtl.mstick() : independent;

DoThing1();
output(rtl.mstick()-StartTime,NAMED('TIME1'));
DoThing2();
output(rtl.mstick()-StartTime-WORKUNIT('TIME1', unsigned4),NAMED('TIME2'));
