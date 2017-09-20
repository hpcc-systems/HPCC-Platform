/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

w(unsigned i) := INLINE MODULE
   export v := i * 10;
END;

x(unsigned i) := INLINE MODULE
    export y := w(i).v * 2;
END;

r := record,maxlength(x(10).y)
    unsigned i;
end;

ds := dataset('x', r, THOR);
output(ds);
