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
output(58d / 100);
output(58d / 100d);
output(nofold(58d) / 100d);
output(58.00d / 100);
output(58.00d / 100d);
output(nofold(58.00d) / 100d);


OUTPUT(truncate(58 * 100 / 100), NAMED('Res1'));
OUTPUT(truncate(58 / 100 * 100), NAMED('Res2'));

OUTPUT(truncate( 58d / 100.0 ), NAMED('TRUNCATE_58d_100_0'));
OUTPUT(truncate( 58d / 100 ), NAMED('TRUNCATE_58d_100'));
OUTPUT(truncate( 58d / 100d ), NAMED('TRUNCATE_58d_100d'));

OUTPUT(truncate(658 * 100 / 100), NAMED('Res1b'));
OUTPUT(truncate(658 / 100 * 100), NAMED('Res2b'));

OUTPUT(truncate( 658d / 100.0 ), NAMED('TRUNCATE_68d_100_0'));
OUTPUT(truncate( 658d / 100 ), NAMED('TRUNCATE_68d_100'));
OUTPUT(truncate( 658d / 100d ), NAMED('TRUNCATE_68d_100d'));
OUTPUT(truncate( 658.00d / 100d ), NAMED('TRUNCATE_68d_100dy'));

OUTPUT(truncate(nofold(58) * 100 / 100), NAMED('Res1cx'));
OUTPUT(truncate(nofold(58) / 100 * 100), NAMED('Res2cx'));

OUTPUT(truncate( nofold(58d) / 100.0 ), NAMED('TRUNCATE_58d_100_0x'));
OUTPUT(truncate( nofold(58d) / 100 ), NAMED('TRUNCATE_58d_100x'));
OUTPUT(truncate( nofold(58d) / 100d ), NAMED('TRUNCATE_58d_100dx'));

OUTPUT(truncate(nofold(658) * 100 / 100), NAMED('Res1dx'));
OUTPUT(truncate(nofold(658) / 100 * 100), NAMED('Res2dx'));


OUTPUT(truncate( nofold(658d) / 100.0 ), NAMED('TRUNCATE_68d_100_0x'));
OUTPUT(truncate( nofold(658d) / 100 ), NAMED('TRUNCATE_68d_100x'));
OUTPUT(truncate( nofold(658d) / 100d ), NAMED('TRUNCATE_68d_100dx'));
