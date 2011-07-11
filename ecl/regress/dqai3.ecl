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

f := nofold(dataset([{'20071001'}],{string8 addr_version_number}));   
  
f_1 := f((unsigned)addr_version_number<20070915);  
f_2 := f((unsigned)addr_version_number>=20070915,  
         (unsigned)addr_version_number<20070920);  
f_3 := f((unsigned)addr_version_number>=20070920,  
         (unsigned)addr_version_number<20071030);  
  
f_out := if(count(f_1)>0,dataset([{1}],{unsigned1 id})) +  
         if(count(f_2)>0,dataset([{2}],{unsigned1 id})) +  
         if(count(f_3)>0,dataset([{3}],{unsigned1 id}));  
           
output(f_out);          
