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

import lib_fileservices;

/*
//Simple output from nothor
nothor(output(FileServices.LogicalFileList('*', true, false)));

//Same, but extending an output
nothor(output(FileServices.LogicalFileList('regress::hthor::simple*', true, false),named('Out2'),extend));
nothor(output(FileServices.LogicalFileList('regress::hthor::house*', true, false),named('Out2'),extend));
*/

//Same, but can't do on own
ds := FileServices.LogicalFileList('*', true, false);
summary := table(nothor(ds), { owner, numFiles := count(group), totalSize := sum(group, size) }, owner);

output(summary);

