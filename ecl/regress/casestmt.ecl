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

#option ('globalFold', false);
export display := SERVICE
 echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;

string x := '1' : stored('x');

case (x, '1'=>display.echo('one'),'2'=>display.echo('two'),'3'=>display.echo('three'),display.echo('many'));

map (x='1'=>display.echo('eins'),x='2'=>display.echo('zwei'),x='3'=>display.echo('drei'),display.echo('xxx'));
