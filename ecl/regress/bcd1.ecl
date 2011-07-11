/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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