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
output(58d / 100);
output(58d / 100d);
output(058d / 100d);
output(116d / 200d);
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
