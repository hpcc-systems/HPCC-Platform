/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

//Test division by zero - fail instead of returning 0
#option ('divideByZero', 'fail'); 

unsigned cintZero := 0;
real crealZero := 0.0;
decimal10_2 cdecZero := 0.0D;

//The constant folding in the pre-processor defaults to throwing an error
#IF (100.0 / crealZero = 0)
OUTPUT('success');
#else
OUTPUT('failure');
#END
