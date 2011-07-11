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

export xRecord := RECORD
data9 zid;
    END;

export xDataset := DATASET('x',xRecord,FLAT);


output(xDataset(zid in [
x'01a48d8414d848e900',
x'01a48d879c3760cf01',
x'01a48ec793a76f9400',
x'01a48ecd6e65d8e803',
x'01a48ed1bb70d84c01',
x'01a48ed8f40385ba01',
x'01a490101d3de9ac02',
x'01a4901558d7c91900',
x'01a491479336b4d500',
x'01a4914d5b326b9202',
x'01a49155d1caf41500',
x'01a49156089fce5a02',
x'01a4916c37db816c02']));
 
 
