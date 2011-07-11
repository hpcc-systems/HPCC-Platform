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
export display := SERVICE
 echo(const string src) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
END;

string x := '1' : stored('x');

case (x, '1'=>display.echo('one'),'2'=>display.echo('two'),'3'=>display.echo('three'),display.echo('many'));

map (x='1'=>display.echo('eins'),x='2'=>display.echo('zwei'),x='3'=>display.echo('drei'),display.echo('xxx'));
