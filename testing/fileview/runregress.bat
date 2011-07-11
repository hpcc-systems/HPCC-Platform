rem @echo off
rem /*##############################################################################
rem 
rem     Copyright (C) 2011 HPCC Systems.
rem 
rem     All rights reserved. This program is free software: you can redistribute it and/or modify
rem     it under the terms of the GNU Affero General Public License as
rem     published by the Free Software Foundation, either version 3 of the
rem     License, or (at your option) any later version.
rem 
rem     This program is distributed in the hope that it will be useful,
rem     but WITHOUT ANY WARRANTY; without even the implied warranty of
rem     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem     GNU Affero General Public License for more details.
rem 
rem     You should have received a copy of the GNU Affero General Public License
rem     along with this program.  If not, see <http://www.gnu.org/licenses/>.
rem ############################################################################## */

call setup.bat
rmdir /s /q out
mkdir out

%eclplus% action=view file=regress::hthor::book > out/book.txt
%eclplus% action=view file=regress::hthor::book filterBy=id:10,id:20 > out/book1.txt
%eclplus% action=view file=regress::hthor::book filterBy=__fileposition__:334,__fileposition__:411 > out/book2.txt
%eclplus% action=view file=regress::hthor::book filterBy=id:10,id:20,__fileposition__:334,__fileposition__:411 > out/book3.txt

%eclplus% action=view file=regress::hthor::house "filterBy=id:1,id:3" > out/house1.txt
%eclplus% action=view file=regress::hthor::house "filterBy=addr:Bedrock,addr:Wimpole Hall" > out/house2.txt
%eclplus% action=view file=regress::hthor::house "filterBy=id:1,id:3,addr:Bedrock,addr:Wimpole Hall" > out/house3.txt

%eclplus% action=view file=regress::hthor::houseIndexID "filterBy=id:1,id:3"  > out/housei1.txt
%eclplus% action=view file=regress::hthor::houseIndexID "filterBy=addr:Bedrock,addr:Wimpole Hall"  > out/housei2.txt
%eclplus% action=view file=regress::hthor::houseIndexID "filterBy=id:1,id:3,addr:Bedrock,addr:Wimpole Hall"  > out/housei3.txt

%eclplus% action=view file=regress::hthor::book format=xml > out/book.xml
%eclplus% action=er file=regress::hthor::house > out/house.er
%eclplus% action=er file=regress::hthor::book > out/book.er
%eclplus% action=er "filepattern=regress::hthor::*" > out/regress.er
%eclplus% action=er files=regress::hthor::house,regress::hthor::person,regress::hthor::book explicitFilesOnly=1 > out/housepersonbook.er
%eclplus% action=viewTree files=regress::hthor::house,regress::hthor::person,regress::hthor::book explicitFilesOnly=1 > out/housepersonbook.tree
%eclplus% action=viewTree files=regress::hthor::house,regress::hthor::person,regress::hthor::book filterBy=id:3 explicitFilesOnly=1 > out/housepersonbook.filter1.tree

%eclplus% action=er files=regress::hthor::houseindexid,regress::hthor::house explicitFilesOnly=1 > out/house1.er
%eclplus% action=viewTree files=regress::hthor::houseindexid,regress::hthor::house explicitFilesOnly=1 > out/house1.tree
%eclplus% action=er files=regress::hthor::personindex,regress::hthor::personindexid explicitFilesOnly=1 > out/person1.er
%eclplus% action=viewTree files=regress::hthor::personindex,regress::hthor::personindexid filterBy=forename:Liz explicitFilesOnly=1 > out/person1.tree
%eclplus% action=viewTree files=regress::hthor::personindex,regress::hthor::personindexid,regress::hthor::person filterBy=forename:Liz explicitFilesOnly=1 > out/person2.tree

%eclplus% !regress::hthor::ts_wordindex filterBy=word:socks    > out/socks1.txt
%eclplus% !regress::hthor::ts_wordindex userFilter=word:socks  > out/socks2.txt
%eclplus% !regress::hthor::ts_wordindex userFilter=word:SOCKS  > out/socks3.txt
%eclplus% !regress::hthor::ts_wordindex userFilter=word:$SOCK?S  > out/socks4.txt

%eclplus% !regress::hthor::ts_searchindex filterBy=word:absalom    > out/absalom1.txt
%eclplus% !regress::hthor::ts_searchindex userFilter=word:absalom  > out/absalom2.txt
%eclplus% !regress::hthor::ts_searchindex userFilter=word:ABSALOM  > out/absalom3.txt

%eclplus% !regress::hthor::dg_varvarindex > out/complex1.txt
%eclplus% !regress::hthor::simplepersonbookindex > out/complex2.txt

%eclplus% view file=REGRESS::hthor::DG_IntegerIndex rangehigh=5
%eclplus% view file=REGRESS::hthor::DG_IntegerIndex filterBy=i6:6
%eclplus% view file=REGRESS::hthor::DG_IntegerIndex filterBy=i5:7
%eclplus% view file=REGRESS::hthor::DG_IntegerIndex filterBy=i3:8



%bc% key out &
