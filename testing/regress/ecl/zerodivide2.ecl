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

#option ('divideByZero', 'zero');

zero := nofold(0.0);

//'INFINITY = '; INFINITY;
'log10(0) = '; log(0.0); log(zero);

'log(0) = '; ln(0.0); ln(zero);
'log10(-1) = '; log(-1.0); log(zero - 1.0);
'log(-1) = '; ln(-1.0); ln(zero - 1.0);
'sqrt(-1) = '; sqrt(-1.0); sqrt(zero - 1.0);
'1/0 = '; 1.0/0.0; 1.0 / zero;
'fmod(1.0, 0.0) = '; 1.0 % 0.0; 1.0 % zero;
'fmod(5.3, 2.0) = '; 5.3 % 2.0; 5.3 % (zero+2.0);
'acos(2) = '; acos(2.0); acos(2.0 + zero);
'asin(2) = '; asin(2.0); asin(2.0 + zero);
