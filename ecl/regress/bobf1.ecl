/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

LOADXML('');
ds := DATASET([{1234},{5678}], {UNSIGNED4 id});

#DECLARE(fieldtype);
#DECLARE(field);

#SET(field, 'ds.id'); //Error: Unknown identifier "ds" (1, 4), 2167,

#SET(fieldtype, #GETDATATYPE(%field%));

output(%'fieldtype'%);
