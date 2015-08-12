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

