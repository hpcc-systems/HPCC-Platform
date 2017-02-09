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

IMPORT Std.Math;
#option ('divideByZero', 'fail');

zero := nofold(0.0);

catch(log(zero), 123);
catch(ln(zero - 1.0), 124);
catch(sqrt(zero - 1.0), 125);
catch(1.0 / zero, 126);
catch(acos(2.0 + zero), 127);
catch(asin(2.0 + zero), 128);
catch(Math.FMod(1.0, zero), 129);
