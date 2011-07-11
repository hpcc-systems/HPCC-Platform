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

//Error : virtual on a global attribute

export unsigned getFibValue(unsigned n) := 0;


export abc := function
    virtual x := 10;            // virtual not allowed in functions
    virtual export y := 20;
    return x*y;
end;


export mm := module
export abc := 10;
export def := 20;
    end;

export mm2 := module(mm)
abc := 10;          // warn - clashes with virtual symbol
export def := 21;
    end;

