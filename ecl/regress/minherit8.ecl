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
export boolean useName;
export boolean useAvailable;
        end;

interface2 := interface
export boolean useName;
export boolean useAddress;
        end;

string f1(interface1 ctx) := '[' + (string)ctx.useName + ',' + (string)ctx.useAvailable + ']';
string f2(interface2 ctx) := '[' + (string)ctx.useName + ',' + (string)ctx.useAddress+ ']';

options1 := module(interface1, interface2)
export boolean useName := true;
export boolean useAvailable := false;
export boolean useAddress := true;
    end;

output(f1(options1));
output(f2(options1));
