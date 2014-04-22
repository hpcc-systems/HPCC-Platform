/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

    This program is free software: you can redistribute it and/or modify
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

bookRec := RECORD
string     title {xpath('bk:title')};
string     author {xpath('bk:author')};
string     rating {xpath('star:rating')};
           END;

collectionRec := RECORD
  dataset(bookRec) books {xpath('bks:books/bk:book')};
end;

xmlinfo := u8'<Row><bks:books><bk:book><bk:author>Sir Roger Penrose</bk:author><bk:title>The Emperors New Mind</bk:title><star:rating>*****</star:rating></bk:book><bk:book><bk:author>Brian Greene</bk:author><bk:title>The Elegant Universe</bk:title><star:rating>*****</star:rating></bk:book><bk:book><bk:author>Stephen Hawking</bk:author><bk:title>A Brief History of Time</bk:title><star:rating>*****</star:rating></bk:book></bks:books></Row>';

books := FROMXML(collectionRec, xmlinfo, TRIM);

output(books, XMLNS('bks','urn:booklist:books'), XMLNS('bk','urn:booklist:book'));

