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

export svc1 :=
    SERVICE
        echo(integer4 value) : eclrtl,library='eclrtl',entrypoint='rtlEcho';
    END;

d := dataset('f::f', {big_endian integer4 f, integer4 g}, thor);

output(sort(d, f),,'g::g');

output(d(f + (big_endian integer4)3 > (big_endian integer4)0),,'g::g2');


apply(d, svc1.echo(f), svc1.echo(g));