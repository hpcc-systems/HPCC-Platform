/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

// this behaves the way I want, but can't I use BCD rather than float?
udecimal8_2 pct_real(string n, string d) := (real)n / (real)d * 100.0;
output( pct_real('1234','10000') );

// the rest of these all evaluate to zero... not sure why
udecimal8_2 pct_bcd(string n, string d) := (udecimal8_4)n / (udecimal9_4)d * 100.0;
output( pct_bcd('1234','10000') );

output( (udecimal8_4)0.1234 * 100 );
output( (udecimal8_4)0.1234 * 100.0 );
output( (udecimal8_4)0.1234 * (udecimal8_4)100 );
output( (udecimal8_4)0.1234 * (udecimal8_4)100.0 );

output( pct_bcd(nofold('1234'),nofold('10000')) );

output( (udecimal8_4)nofold(0.1234) * 100 );
output( (udecimal8_4)nofold(0.1234) * 100.0 );
output( (udecimal8_4)nofold(0.1234) * (udecimal8_4)nofold(100) );
output( (udecimal8_4)nofold(0.1234) * (udecimal8_4)nofold(100.0) );