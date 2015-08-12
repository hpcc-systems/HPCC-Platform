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

export display :=
    SERVICE
        echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;


x := (integer8)'1234567891234567';


display.echo('$$$$'+(string)(x+0)+'$$$$');

t1 :=
        (string)1234 + ' ' + (string)(integer8)1234 + ' ' +
        (string)(unsigned4)1234 + ' ' + (string)(unsigned8)1234 + ' ' +
        (string)1234.56 + ' ' + (string)(decimal10_2)1234.56;

t2 :=
        (varstring)1234 + ' ' + (varstring)(integer8)1234 + ' ' +
        (varstring)(unsigned4)1234 + ' ' + (varstring)(unsigned8)1234 + ' ' +
        (varstring)1234.56 + ' ' + (varstring)(decimal10_2)1234.56;


display.echo(t1 + t2);

/* different forms of subrange */
display.echo((string)('Gavin'[1]+'Hawthorn'));
display.echo((string)('Gavin'[1..]+'Hawthorn'));
display.echo((string)('Gavin'[1..3]+'Hawthorn'));
display.echo((string)('Gavin'[..3]+'Hawthorn'));

/* different forms of subrange with constant fold */
display.echo((string)('Gavin'[1+1]+'Hawthorn'));
display.echo((string)('Gavin'[1+1..]+'Hawthorn'));
display.echo((string)('Gavin'[1+1..3+0]+'Hawthorn'));
display.echo((string)('Gavin'[..3+0]+'Hawthorn'));
