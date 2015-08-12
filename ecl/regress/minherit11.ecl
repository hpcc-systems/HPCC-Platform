/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
