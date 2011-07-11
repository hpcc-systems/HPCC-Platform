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

string wget(string url) := FUNCTION

  docrec := RECORD
    STRING s;
  END;

  lines := PIPE('wget.exe -q -O - ' + url, docrec, csv(separator([]),quote([])), repeat, group);

  RETURN AGGREGATE(lines, docrec, transform(docrec, SELF.s := RIGHT.s + LEFT.s))[1].s;
END;

crawlrec := RECORD
  STRING url;
  STRING contents;
END;

urls := dataset([{'http://www.google.com'}, {'http://www.somewhereelse.org'}], {string url});

crawled := project(urls, TRANSFORM(crawlrec, SELF.url := LEFT.url, self.contents := wget(LEFT.url)));
output(crawled);
count(crawled)

