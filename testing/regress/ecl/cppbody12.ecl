/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

real storedval := 1 : stored('s');

real scoreFunc(real a, real b) := 1;   // defines the prototype for the function argument

real scoreIt(scoreFunc func, real a, real b) := BEGINC++
    return func(ctx,a-1.0,b-1.0) * func(ctx,a+1.0,b+1.0);
ENDC++;
    
real doSum(real a, real b) := DEFINE (a + b + storedval);

ds := DATASET(100, transform({unsigned id}, SELF.id := COUNTER));
c := COUNT(NOFOLD(ds));

real doSum2(real a, real b) := DEFINE (a + b + c);

output(scoreIt(doSum, 10, 20));
output(scoreIt(doSum2, 100, 200));
output(scoreIt(doSum2, 1, 3));
