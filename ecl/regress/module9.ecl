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

import Std.Str;
import Std.Str.^.Uni;


x(boolean good) := FUNCTION
    m0 := MODULE
        export f0 := 'ok';
    END;
    m1 := MODULE(m0)
        export f1 := 'hello';
    END;
    m2 := MODULE(m0)
        export f2 := 'goodbye';
    END;
    m := IF(good, m1, m2);
    RETURN m.f0;
END;

x(true);
x(false);
