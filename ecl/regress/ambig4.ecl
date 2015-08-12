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

//Here to illustrate the problem with ambiguous expressions caused by the way that a.b.c is handled.
//The attribute hasLongChapters has 3 different meanings depending on the context it is used in.

chapterRecord := RECORD
   unsigned4 id;
   unsigned4 pages;
END;

bookRecord := RECORD
    UNSIGNED4 id;
    DATASET(chapterRecord) chapters;
END;

libraryRecord := RECORD
    UNSIGNED4 id;
    DATASET(bookRecord) books;
END;

libraries := nofold(dataset([
       {1,
            [{1,
                [{ 1, 5 }, { 2, 10 }, { 3, 7 }]
            }]
       },
       {2,
            [{1,
                [{ 1, 20 }, { 2, 30 }, { 3, 50 }]
             },
             {2,
                [{ 1, 12 }, { 2, 10 }, { 3, 7 }]
             },
             {3,
                [{ 1, 2 }, { 2, 5 }, { 3, 7 }]
            }]
       },
       {999, [] }
           ], libraryRecord));

longChapters := libraries.books.chapters(pages > 10);

hasLongChapters := count(longChapters) > 0;

output(hasLongChapters);
output(libraries(hasLongChapters), { id } );
output(libraries.books(hasLongChapters), { id } );
