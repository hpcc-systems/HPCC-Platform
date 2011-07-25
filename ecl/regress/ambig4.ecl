/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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
