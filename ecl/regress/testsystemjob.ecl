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

import std.system.job;

//Would be nice if we could have a fully qualified path e.g.,
//output(#text(x) + ' = ' + system.job.x + '\n')

show(x) := MACRO
output(#text(x) + ' = ' + job.x() + '\n')
ENDMACRO;

show(wuid);
show(daliServer);
show(name);
show(user);
show(target);
show(platform);
show(os);
evaluate(job.logString('This is a line of tracing'));

