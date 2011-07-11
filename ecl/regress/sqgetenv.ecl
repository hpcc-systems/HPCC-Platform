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

#option ('childQueries', true);
#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',true);
#option ('newChildQueries', true);
#option ('targetClusterType', 'roxie');
#option ('hoistResourced', true);

sys := service
string getEnvironment(string id) : ctxmethod,pure,entrypoint='getEnvironment';
       end;

import sq;
sq.DeclareCommon();


output(sys.getEnvironment('Version'));

ded := dedup(sqHousePersonBookDs.persons(forename != sys.getEnvironment('Forename')), forename);

p := table(sqHousePersonBookDs, { id, dataset children := ded });
output(p);
