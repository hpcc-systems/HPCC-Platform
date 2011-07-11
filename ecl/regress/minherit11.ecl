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

//Example to show deriving module attributes that include scopes.  Doesn't currently work for several reasons.
//and may never work...
//1) Need to use the unresolved module in the definition of ibyname, but can cause r/r error.
//2) Need to allow scopes to be overriden - problems with queryScope() on no_delayedselect (see minherit11.hql)

interface1 := module,virtual
export boolean useName := false;
export boolean useAvailable := false;
        end;

interface2 := module,virtual
export boolean useName := false;
export boolean useAddress := false;
        end;

interface3 := interface
export ibyname := interface1;
export ibyaddr := interface2;
    end;

string f1(interface1 ctx) := '[' + (string)ctx.useName + ',' + (string)ctx.useAvailable + ']';
string f2(interface2 ctx) := '[' + (string)ctx.useName + ',' + (string)ctx.useAddress+ ']';
string f3(interface3 ctx) := f1(ctx.ibyname) + f2(ctx.ibyaddr);


options := module(interface3)

export ibyname := 
                module(interface1)
    export boolean useName := true;
    export boolean useAvailable := false;
                end;

export ibyaddr := 
                module(interface2)
    export boolean useName := true;
    export boolean useAddress := true;
                end;
        end;

output(f3(options));
