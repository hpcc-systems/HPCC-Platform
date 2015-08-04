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

//Error : recursive value

m1 := MODULE,virtual
export unsigned getFibValue(unsigned n) := 0;
export unsigned fibValue(unsigned n) := IF(n = 1, 1, getFibValue(n-1) + getFibValue(n-2));
        END;


m2 := MODULE(m1)
export unsigned getFibValue(unsigned n) := fibValue(n);
        END;


output(m2.getFibValue(5));          //recursive function definition, even through theoretically evaluatable
