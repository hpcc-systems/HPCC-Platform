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

activityRecord := RECORD
    UNSIGNED id{XPATH('@id')};
    STRING label{XPATH('@label')};
    UNSIGNED kind{XPATH('att[name="kind"]/@value')};
END;

subGraphRecord := RECORD
    UNSIGNED id{XPATH('@id')};
    DATASET(activityRecord) activities{XPATH('att/graph/node')};
END;

graphRecord := RECORD
    STRING name{XPATH('@name')};
    DATASET(subGraphRecord) subgraphs{XPATH('xgmml/graph/node')};
END;


ds := PIPE('cmd /C type c:\\temp\\a.xml', graphRecord, xml('*/Graphs/Graph'));
output(ds);

ds1 := DATASET('c:\\temp\\a.xml', graphRecord, xml('*/Graphs/Graph'));
output(ds1);
