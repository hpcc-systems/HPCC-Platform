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

integer dd1 := 123;

STRING10 eee1 := (STRING)dd1 + 'TEST';

display.echo('$'+eee1+'$');

integer dd2 := 123;

STRING10 eee2 := (STRING4)dd2 + 'TEST';

display.echo('$'+eee2+'$');

integer dd3 := 123+0;

STRING10 eee3 := (STRING)dd3 + 'TEST';

display.echo('$'+eee3+'$');

integer dd4 := 123+0;

STRING10 eee4 := (STRING4)dd4 + 'TEST';

display.echo('$'+eee4+'$');

