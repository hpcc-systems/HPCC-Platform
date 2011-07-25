/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

subGraph := RECORD
    UNSIGNED id{XPATH('@id')};
END;

graphRecord := RECORD
    STRING name{XPATH('@name')};
    DATASET(subGraph) subgraphs{XPATH('xgmml/graph/node')};
END;


ds := DATASET([{'graph1',[{1},{2},{3},{4}]},{'graph2',[{20},{40},{60}]}], graphRecord);
output(ds);
