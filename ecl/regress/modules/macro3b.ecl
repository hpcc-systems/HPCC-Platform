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

RETURN MODULE

import macro3a;

export macro2(inf,outf) := macro
inf into_2(inf L) := transform
self.lname := 'MARONEY';
self := l;
end;

temp := project(inf,into_2(LEFT));

macro3a.macro1(temp,outf)

endmacro;

END;