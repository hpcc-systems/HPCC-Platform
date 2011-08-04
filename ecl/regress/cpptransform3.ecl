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

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

outRecord := RECORD
               STRING fullname{maxlength(30)};
             END;

transform(outRecord) createName(namesRecord l) := BEGINC++
    size32_t len1 = rtlTrimStrLen(20, (const char *)l);
    size32_t len2 = rtlTrimStrLen(10, (const char *)(l+20));
    size32_t len = len1+1+len2+1;
    byte * self = __self.ensureCapacity(sizeof(size32_t)+len,"");
    *(size32_t *)self = len;
    memcpy(self+sizeof(size32_t), l, len1);
    memcpy(self+sizeof(size32_t)+len1, " ", 1);
    memcpy(self+sizeof(size32_t)+len1+1, l+20, len2);
    memcpy(self+sizeof(size32_t)+len1+1+len2, "\n", 1);
    return sizeof(size32_t)+len;
ENDC++;

output(project(namesTable2, createName(LEFT)));
