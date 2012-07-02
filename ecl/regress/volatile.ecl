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

import Std.System.Debug;

startTime := Debug.msTick() : independent;

output(startTime-Debug.msTick());
Debug.Sleep(10);

output(startTime-Debug.msTick());
Debug.Sleep(10);

//Evaluate this once
output(startTime-Debug.msTick());

//Evaluate tick twice
output(startTime*startTime-Debug.msTick()*Debug.msTick());

//Only evalaute tick once
now := Debug.msTick();
output(startTime*startTime-now*now);


nowTime() := define Debug.msTick();

//Evaluate nowTime twice
output(startTime*startTime-nowTime()*nowTime());

//Only evalaute nowTime once
now2 := nowTime();
output(startTime*startTime-now2*now2);
