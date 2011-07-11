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


integer aveValues(integer v1, integer v2) := (v1 + v2) DIV 2;
integer addValues(integer v1, integer v2) := v1 + v2;
integer multiValues(integer v1, integer v2) := v1 * v2;

integer applyValue2(integer v1, integer actionFunc(integer v1, integer v2) = aveValues) := actionFunc(v1, v1+1)*2;
integer applyValue4(integer v1, integer actionFunc(integer v1, integer v2) = aveValues) := actionFunc(v1, v1+1)*4;
integer doApplyValue(integer v1, integer actionFunc(integer v1, integer v2)) := applyValue2(v1+1, actionFunc);

output(applyValue2(1));                 // 2
output(applyValue2(2));                 // 4
output(applyValue2(1, addValues));      // 6
output(applyValue2(2, addValues));      // 10
output(applyValue2(1, multiValues));        // 4
output(applyValue2(2, multiValues));        // 12

output(doApplyValue(1, multiValues));       // 12
output(doApplyValue(2, multiValues));       // 24


//Phew: An attribute taking functional parameters which themselves have parameters which are functional...
//check it all binds correctly
string doMany(integer v1, 
                integer firstAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer secondAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer actionFunc(integer v1, integer v2)) := 
                (string)firstAction(v1, actionFunc) + ':' + (string)secondaction(v1, actionFunc);


output(doMany(1, applyValue2, applyValue4, addValues));     // "6:12"
output(doMany(2, applyValue4, applyValue2, multiValues));   // "24:12"
