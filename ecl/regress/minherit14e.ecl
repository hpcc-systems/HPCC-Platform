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

