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


en_rec := RECORD
    INTEGER1 num;
    UNICODE_en str;
    DATA key := KEYUNICODE(SELF.str);
END;

en_set := DATASET([{9, u'zoology'}, {3, u'cat'}, {1, 'ant'}, {4, u'llama'}, {6, 'maria'}, {5, 'lucia'}, {2, u'bag'}, {7, u'xylophone'}, {8, 'yacht'}], en_rec);

es_rec := RECORD
    INTEGER1 num;
    UNICODE_es__TRADITIONAL str;
    DATA key;
END;

es_rec trans(en_rec l) := TRANSFORM
    SELF.str := (UNICODE_es__TRADITIONAL)l.str;
    SELF.key := KEYUNICODE(SELF.str);
    SELF := l;
END;

es_set := PROJECT(en_set, trans(LEFT));

'en, unicode compare (should appear in numerical order)';

OUTPUT(SORT(en_set, str));

'en, key compare (should appear in numerical order)';

OUTPUT(SORT(en_set, key));

'es, unicode compare (should exchange 4 and 5)';

OUTPUT(SORT(es_set, str));

'es, key compare (should exchange 4 and 5)';

OUTPUT(SORT(es_set, key));
