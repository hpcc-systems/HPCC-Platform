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


import sq;
sq.DeclareCommon();
#option ('optimizeDiskSource',false);

infile := dataset(sqHousePersonBookName, sqHousePersonBookIdRec, thor);
d := distribute(infile, hash(yearBuilt));
//output(d,,'~sqHousePersonBookXml', xml, overwrite);


xmlrec := record(sqHousePersonBookIdRec)
unsigned8           filepos{virtual(fileposition)};
//unsigned8         localfilepos{virtual(localfileposition)};
        end;

inxml := dataset('~sqHousePersonBookXml', xmlrec, xml('Dataset/Row'));
output(inxml);
