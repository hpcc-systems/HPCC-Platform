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


unsigned addValues(integer v1, integer v2) := v1 + v2;
integer multiValues(string v1, integer v2) := (unsigned)(v1[1]) * v2;

integer applyValue(integer v1, integer actionFunc(integer v1, integer v2)) := actionFunc(v1, v1+1)*2;
integer doApplyValue(integer v1, integer actionFunc(integer v1, integer v2)) := applyValue(v1+1, actionFunc);
integer applyValueX(integer v1, integer actionFunc(string v1, integer v2)) := actionFunc((string)v1, v1+1)*2;

output(applyValue(1, addValues));       // not compatible - ret type
output(applyValue(2, addValues));       // not compatible - ret type
output(applyValue(1, multiValues));     // not compatible - argument
output(applyValue(2, multiValues));     // not compatible - argument

output(doApplyValue(1, multiValues));       // not compatible
output(doApplyValue(2, multiValues));       // not compatible
output(doApplyValue(multiValues, 1));       // func passed to non func
output(doApplyValue(2, 3));                 // non func passed to func


//Phew: An attribute taking functional parameters which themselves have parameters which are functional...
//check it all binds correctly
string doMany(integer v1, 
                integer firstAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer secondAction(integer v1, integer actionFunc(integer v1, integer v2)),
                integer actionFunc(integer v1, integer v2)) := 
                (string)firstAction(v1, actionFunc) + ':' + (string)secondaction(v1, actionFunc);


output(doMany(2, applyValue, applyValueX, multiValues));    // Second is incompatible - deeply nested difference


integer applyValue2(integer v1, integer actionFunc(integer v1, integer v2) = addValues) := actionFunc(v1, v1+1)*2;


output(applyValue2(1));     // default is not compatible
