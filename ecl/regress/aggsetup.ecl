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

import AggCommon;
AggCommon.CommonDefinitions();

d := dataset([{'Halliday','Gavin',35,['Pride and Predudice','Treasure Island']},
              {'Halliday','Abigail',2,['Where the wild things are','Maisy']},
              {'Smith','John',57,['Budgetting']},
              {'Smith','Gavin',12,['100 things to do with a dead cat','The bookseller of Kabul']}
              ], namesRecord);


output(d,,aggTableName,overwrite);
buildindex(i,overwrite);

