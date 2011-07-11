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

#option ('importAllModules', true);


r := record
 DATASET({EBCDIC STRING30 NAME},count(10)) MTG_LOAN1 {MAXCOUNT(10)}; 
 DATASET({EBCDIC STRING30 NAME},count(10)) MTG_LOAN2 {MAXCOUNT(10)}; 
 DATASET({EBCDIC STRING15 NAME},count(10)) MTG_LOAN3 {MAXCOUNT(10)}; 
 DATASET({EBCDIC STRING15 NAME},count(10)) MTG_LOAN4 {MAXCOUNT(10)};
end;


output(dataset('x',r,FLAT));
