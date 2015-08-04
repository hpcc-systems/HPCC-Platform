/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

string wget(string url) := FUNCTION

  docrec := RECORD
    STRING s;
  END;

  string wgetExe := 'wget.exe';

  lines := PIPE(wgetExe +' -q -O - ' + url, docrec, csv(separator([]),quote([]),terminator([])));

  RETURN lines[1].s;
END;

crawlrec := RECORD
  STRING url;
  STRING contents;
END;

urls := dataset([{'http://www.google.com'}, {'http://www.somewhereelse.org'}], {string url});

crawled := project(urls, TRANSFORM(crawlrec, SELF.url := LEFT.url, self.contents := wget(LEFT.url)));
output(crawled);
count(crawled)
