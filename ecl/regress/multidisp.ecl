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

import lib_fileservices;

rec := RECORD
      STRING10 S;
       END;
       
srcnode := '10.150.199.2';
srcdir  := '/c$/test/';

dir := FileServices.RemoteDirectory(srcnode,srcdir,'*.txt',true);

SEQUENTIAL(
FileServices.DeleteSuperFile('MultiSuper1'),
FileServices.CreateSuperFile('MultiSuper1'),
FileServices.StartSuperFileTransaction(),
apply(dir,FileServices.AddSuperFile('MultiSuper1',name,,true)),
FileServices.FinishSuperFileTransaction()
);
