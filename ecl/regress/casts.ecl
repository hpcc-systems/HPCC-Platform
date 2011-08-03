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
