/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

Layout := {
  unsigned id,
};

Model := module, virtual
  dataset(layout) GetInvalid := dataset([], layout);
end;

DocInfo := module(Model) // no syntax error; no runtime error; bad results

  export dsLni := dataset([{1},{3}], layout);
  export dskey:= dataset([{2},{3}], layout);
  export keyDocId := index(dskey, {id}, {dskey}, '~dustin::delete::docId');

  export GetInvalid
    := join(dsLni, keyDocId,
            left.id = right.id,
            transform(right),
            right only);
end;

sequential(
  buildindex(DocInfo.keyDocid),
  output(DocInfo.GetInvalid)
);
