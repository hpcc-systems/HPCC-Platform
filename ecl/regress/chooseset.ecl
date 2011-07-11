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

zpr := 
            RECORD
string20        per_cms_pname;
string20        per_cms_qname := '';
            END;

zperson := dataset([{'a'}], zpr);


x := choosesets(zperson, per_cms_pname='Gavin'=>100,per_cms_pname='Richard'=>20,per_cms_pname='Liz'=>1000,94);
output(x,,'out1.d00');

y := choosesets(zperson, per_cms_pname='Gavin'=>100,per_cms_pname='Richard'=>20,per_cms_pname='Liz'=>1000,94,EXCLUSIVE);
output(y,,'out1.d00');
