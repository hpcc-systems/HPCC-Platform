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

// Nasty - an enum defined inside a module - the values for the enum need to be bound later.

myModule(unsigned4 baseError, string x) := MODULE

export ErrorBase := baseError;
export ErrNoActiveTable := ErrorBase+1;
export ErrNoActiveSystem := ErrNoActiveTable+1;
export ErrFatal := ErrNoActiveSystem+1;

export reportX := FAIL(ErrNoActiveTable, 'No ActiveTable in '+x);
end;




//myModule(100, 'Call1').reportX;

//myModule(300, 'Call2').reportX;


//output(ErrorCodes.ErrFatal);
output(myModule(1999, 'Call4').ErrFatal);
