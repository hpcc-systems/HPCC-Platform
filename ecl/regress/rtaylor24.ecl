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

InRec := RECORD
    string UIDstr;
    string LeftInStr;
    string RightInStr;
END;
InDS := DATASET([
{'1','the quick brown fox jumped over the lazy red dog','quick fox red dog'},
{'2','the quick brown fox jumped over the lazy red dog','quick fox black dog'},
{'3','george of the jungle lives here','fox black dog'},
{'4','fred and wilma flintstone','fred flintstone'},
{'5','osama obama yomama comeonah','barak hillary'}
],InRec);
output(InDS,,'RTTEST::TEST::CSVHeaderTest',CSV(HEADING(SINGLE)),overwrite);

