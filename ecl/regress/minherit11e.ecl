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



interface1 := module,interface
export string50 firstName;
export string50 lastName;
        end;

string f1(interface1 ctx) := '[' + ctx.firstName + ',' + ctx.lastName + ']';

options := module(interface1)
export string10 firstName := 'Gavin';       //incompatible explicit types
export string lastName := 'Halliday';       //incompatible explicit types
    end;



output(f1(options));
