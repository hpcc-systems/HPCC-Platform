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



assert([1] = [1] + []);
assert([1, 2] = [1] + [2]);
assert([1, 2] = [[1], [2]]);
assert([1, 2, 3] = [[1], [[2], 3]]);

integer stored_9 := 9 : stored('stored_9');
set of integer stored_fib := [1,2,3,5,8] : stored('stored_fib');

assert([stored_9] + [4] + stored_fib = [stored_9, 4, stored_fib]);
assert([stored_9] + ([4] + stored_fib) = [stored_9, 4, stored_fib]);
