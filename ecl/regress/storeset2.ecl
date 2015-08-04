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

r := record
  string1000 line;
  unsigned integer8 __filepos { virtual(fileposition)};
end;

KJV_File := dataset('~thor::in::ktext',r,csv(separator('')));

df := kjv_file;

rs := record
  integer1 book := 0;
  unsigned1 chapter := 0;
  unsigned1 verse := 0;
  unsigned8 __fpos;
end;

d := dataset('hi',rs,csv(separator('')));

Key_KJV_File := INDEX(d,{book,chapter,verse,__fpos},'~thor::key.kjv.verse');

set of integer2 favouriteBooks := [1,69,71,257] : stored('favouriteBooks');

i := Key_KJV_File(book in favouriteBooks, chapter in favouriteBooks);

myline := record
  string200 line;
  unsigned8 filepos;
END;

myline get(df le, i ri) := transform
  self.filepos := ri.__fpos;
  self.line := le.line;
  end;

fd := fetch(df,i,right.__fpos,get(left, right));

output(sort(fd, filepos), { line });
