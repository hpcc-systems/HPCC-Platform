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


testModule := MODULE

  EXPORT Original := MODULE
    EXPORT Layout := RECORD
      STRING v;
    END;

    EXPORT File := DATASET([{'a'}], Layout);
  END;


  EXPORT parent := MODULE, VIRTUAL
    SHARED integer Trans(Original.Layout L) := TRANSFORM
      SELF := L;
    END;

    EXPORT File := PROJECT(Original.File, Trans(LEFT));
  END;


  EXPORT child := MODULE(parent)
    SHARED Original.Layout Trans(Original.Layout L) := TRANSFORM
      SELF.v := L.v + '2';
    END;
  END;

END;

testmodule.child.file;
