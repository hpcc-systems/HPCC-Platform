/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
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
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick';
 unsigned4 sleep(unsigned4 _delay) : eclrtl,library='eclrtl',entrypoint='rtlSleep';
END;


x := _INSTANCE_(rtl.msTick());
y := rtl.msTick();

evaluate(x); rtl.Sleep(10); output(y - x);
