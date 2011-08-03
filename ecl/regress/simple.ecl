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

