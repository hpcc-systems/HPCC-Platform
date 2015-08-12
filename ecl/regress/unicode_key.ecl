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
