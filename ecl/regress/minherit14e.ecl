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

//Test errors when redefining non-virtual attributes.  Ensures we can make all modules virtual later without breaking anything.

baseModule := module
export boolean useName := false;
export boolean useAvailable := false;
        end;



abc := module(baseModule)
export boolean useName := true;         // redefinition error - neither base nor this class is virtual
        end;

def := module(baseModule),virtual
export boolean useName := true;         // base isn't virtual, should this be an error or not?
        end;

