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

xRecord := RECORD
data9 per_cid;
    END;

yRecord := RECORD
xRecord;
unsigned8 __filepos{VIRTUAL(FILEPOSITION)}
    END;

aRecord := RECORD
unsigned8 __filepos{VIRTUAL(FILEPOSITION)};
xRecord;
    END;

xDataset := DATASET('x',xRecord,FLAT);
output(xDataset,, 'outx.d00');
 
yDataset := DATASET('y',yRecord,FLAT);
output(yDataset,{per_cid,__filepos}, 'outy.d00');
 
output(yDataset,,'outz.d00');

aDataset := DATASET('a',aRecord,FLAT);
output(aDataset,, 'outa.d00');
