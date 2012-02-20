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

LOADXML('<xml/>');

#DECLARE (mstg)
#DECLARE (dmstg)

#SET (mstg, #MANGLE('ABC_DEF/AREACODES'));
export res1 := %'mstg'%;
res1; //res1 = 'ABC_5fDEF_2fAREACODES'

// Do some processing with ECL Valid Label name "mstg"

#SET (dmstg, #DEMANGLE(%'mstg'%));
export res2 := %'dmstg'%;
res2; //res2 = 'ABC_DEF/AREACODES'
