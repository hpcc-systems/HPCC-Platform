/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


RecDef := 
            RECORD
integer2        idx := 0;
string2         cc := '??';
string50        name := '??????????';
            END;

NameTable := dataset([
  {   2, 'nc','New Caledonia' },
  {   3, 'lk','Sri Lanka' },
  {   4, 'sm','San Marino' },
  {   5, 'la','Lao People\'s Democratic Republic' },
  {   6, 'sv','El Salvador' },
  {   7, 'st','Sao Tome and Principe' },
  {   8, 'nz','New Zealand' },
  {  36, 'il','Israel' },
  {  10, 'so','Somalia' },
  {  42, 'cu','Cuba' },
  {  15, 'sd','Sudan' },
  {  38, 'hr','Croatia/Hrvatska' },
  {  25, 'mg','Madagascar' },
  {  18, 'np','Nepal' },
  {  24, 'mo','Macau' },
  {  17, 'al','Albania' },
  {  14, 'ro','Romania' },
  {  23, 'mw','Malawi' },
  {  30, 'kz','Kazakhstan' },
  {  27, 'ua','Ukraine' },
  {  28, 'lb','Lebanon' },
  {  22, 'my','Malaysia' },
  {  29, 'pl','Poland' },
  {  33, 'jp','Japan' },
  {  20, 'tv','Tuvalu' },
  {  13, 'pw','Palau' },
  {  41, 'ec','Ecuador' },
  {  34, 'jm','Jamaica' },
  {  16, 'pa','Panama' },
  {   9, 'aq','Antarctica' },
  {  31, 'bs','Bahamas' },
  {  11, 'qa','Qatar' },
  {  21, 'mz','Mozambique' },
  {  37, 'ca','Canada' },
  {  40, 'gq','Equatorial Guinea' },
  {  39, 'gy','Guyana' },
  {  19, 'ni','Nicaragua' },
  {  26, 'mc','Monaco' },
  {  32, 'kw','Kuwait' },
  {  12, 'py','Paraguay' },
  {  35, 'by','Belarus' },
  {  46, 'gm','Gambia' },
  {  47, 'cs','Serbia and Montenegro' },
  {  44, 'zw','Zimbabwe' },
  {  48, 'dj','Djibouti' },
  {  50, 'bb','Barbados' },
  {  43, 'aw','Aruba' },
  {  49, 'kh','Cambodia' },
  {  45, 'zm','Zambia' },
  {  54, 'cz','Czech Republic' },
  {  51, 'li','Liechtenstein' },
  {  52, 'sc','Seychelles' },
  {  53, 'pn','Pitcairn Island' },
  {  56, 'jo','Jordan' },
  {  60, 'gp','Guadeloupe' },
  {  57, 'se','Sweden' },
  {  59, 'sa','Saudi Arabia' },
  {  58, 'hn','Honduras' },
  {  61, 'td','Chad' },
  {  62, 'mv','Maldives' },
  {  55, 'md','Moldova,Republic of' },
  {  70, 'am','Armenia' },
  {  68, 'ng','Nigeria' },
  {  65, 'dz','Algeria' },
  {  76, 'tk','Tokelau' },
  {  63, 'ps','Palestinian Territories' },
  {  69, 'ne','Niger' },
  {  75, 'lr','Liberia' },
  {  79, 'uz','Uzbekistan' },
  {  73, 'cv','Cape Verde' },
  {  81, 'im','Isle of Man' },
  {  64, 'ar','Argentina' },
  {  67, 'sn','Senegal' },
  {  85, 'cm','Cameroon' },
  {  71, 'ac','Ascension Island' },
  {  72, 'mk','Macedonia,The Former Yugoslav Republic of' },
  {  84, 'gl','Greenland' },
  {  80, 've','Venezuela' },
  {  77, 'kr','Korea,Republic of' },
  {  74, 'lu','Luxembourg' },
  {  66, 'nu','Niue' },
  {  83, 'gr','Greece' },
  {  86, 'ci','Cote d\'Ivoire' },
  {  78, 'kp','Korea,Democratic People\'s Republic' },
  {  82, 'ye','Yemen' },
  {  87, 'nf','Norfolk Island' },
  {  91, 'uy','Uruguay' },
  { 101, 'cg','Congo,Republic of' },
  {  96, 'sg','Singapore' },
  {  90, 'bd','Bangladesh' },
  {  88, 'bg','Bulgaria' },
  {  93, 'mn','Mongolia' },
  { 100, 'hu','Hungary' },
  {  94, 'cd','Congo,The Democratic Republic of the' },
  {  98, 'vg','Virgin Islands,British' },
  {  89, 'be','Belgium' },
  {  95, 'hk','Hong Kong' },
  {  99, 'vi','Virgin Islands,US' },
  {  92, 'to','Tonga' },
  {  97, 'kg','Kyrgyzstan' },
  { 103, 'af','Afghanistan' },
  { 102, 'nl','Netherlands' },
  { 105, 'lt','Lithuania' },
  { 104, 'an','Netherlands Antilles' },
  { 109, 'ag','Antigua and Barbuda' },
  { 110, 'tn','Tunisia' },
  { 116, 'in','India' },
  { 112, 'bj','Benin' },
  { 125, 'na','Namibia' },
  { 117, 'th','Thailand' },
  { 108, 'cx','Christmas Island' },
  { 115, 'dm','Dominica' },
  { 114, 'do','Dominican Republic' },
  { 119, 'tj','Tajikistan' },
  { 126, 'bo','Bolivia' },
  { 107, 'ml','Mali' },
  { 120, 'sr','Suriname' },
  { 122, 'et','Ethiopia' },
  { 121, 'es','Spain' },
  { 118, 'ki','Kiribati' },
  { 123, 'fj','Fiji' },
  { 106, 'sy','Syrian Arab Republic' },
  { 124, 'mx','Mexico' },
  { 111, 'pk','Pakistan' },
  { 113, 'bz','Belize' },
  { 127, 'tr','Turkey' },
  { 131, 'bf','Burkina Faso' },
  { 128, 'tc','Turks and Caicos Islands' },
  { 130, 'fk','Falkland Islands (Malvinas)' },
  { 129, 'ck','Cook Islands' },
  { 132, 'tm','Turkmenistan' },
  { 140, 'ie','Ireland' },
  { 136, 'cl','Chile' },
  { 133, 'sj','Svalbard and Jan Mayen Islands' },
  { 134, 'fi','Finland' },
  { 138, 'wf','Wallis and Futuna Islands' },
  { 135, 'ph','Philippines' },
  { 139, 'it','Italy' },
  { 137, 'is','Iceland' },
  { 144, 'dk','Denmark' },
  { 145, 'de','Germany' },
  { 143, 'ky','Cayman Islands' },
  { 142, 'gu','Guam' },
  { 141, 'bm','Bermuda' },
  { 168, 'om','Oman' },
  { 165, 'gn','Guinea' },
  { 159, 'bn','Brunei Darussalam' },
  { 163, 'gd','Grenada' },
  { 158, 'lc','Saint Lucia' },
  { 164, 'kn','Saint Kitts and Nevis' },
  { 148, 'gw','Guinea-Bissau' },
  { 154, 'ir','Iran,Islamic Republic of' },
  { 169, 'ax','Aland Islands' },
  { 166, 'ba','Bosnia and Herzegovina' },
  { 152, 're','Reunion Island' },
  { 146, 'mm','Myanmar' },
  { 151, 'rw','Rwanda' },
  { 167, 'pf','French Polynesia' },
  { 162, 'gf','French Guiana' },
  { 149, 'sh','Saint Helena' },
  { 153, 'ug','Uganda' },
  { 161, 'vc','Saint Vincent and the Grenadines' },
  { 157, 'tf','French Southern Territories' },
  { 155, 'pm','Saint Pierre and Miquelon' },
  { 150, 'gh','Ghana' },
  { 147, 'tt','Trinidad and Tobago' },
  { 156, 'fr','France' },
  { 160, 'cn','China' },
  { 180, 'yu','Yugoslavia' },
  { 184, 'yt','Mayotte' },
  { 182, 'fo','Faroe Islands' },
  { 181, 'ee','Estonia' },
  { 177, 'ga','Gabon' },
  { 174, 'km','Comoros' },
  { 179, 'sb','Solomon Islands' },
  { 178, 'ls','Lesotho' },
  { 185, 'id','Indonesia' },
  { 183, 'tl','Timor-Leste' },
  { 172, 'ao','Angola' },
  { 186, 'ad','Andorra' },
  { 173, 'cc','Cocos (Keeling) Islands' },
  { 175, 'co','Colombia' },
  { 170, 'tg','Togo' },
  { 171, 'ma','Morocco' },
  { 176, 'eu','European Union' },
  { 187, 'eg','Egypt' },
  { 188, 'iq','Iraq' },
  { 200, 'sl','Sierra Leone' },
  { 199, 'cy','Cyprus' },
  { 195, 'hm','Heard and McDonald Islands' },
  { 196, 'mr','Mauritania' },
  { 192, 'nr','Nauru' },
  { 197, 'fm','Micronesia,Federal State of' },
  { 194, 'az','Azerbaijan' },
  { 201, 'mu','Mauritius' },
  { 191, 'gg','Guernsey' },
  { 202, 'bh','Bahrain' },
  { 193, 'ge','Georgia' },
  { 198, 'gi','Gibraltar' },
  { 189, 'pr','Puerto Rico' },
  { 190, 'as','American Samoa' },
  { 206, 'ru','Russian Federation' },
  { 204, 'mh','Marshall Islands' },
  { 205, 'je','Jersey' },
  { 203, 'bw','Botswana' },
  { 216, 'mt','Malta' },
  { 212, 'za','South Africa' },
  { 208, 'gt','Guatemala' },
  { 213, 'gs','South Georgia and the South Sandwich Islands' },
  { 232, 'cr','Costa Rica' },
  { 211, 'eh','Western Sahara' },
  { 220, 'tp','East Timor' },
  { 223, 'ws','Western Samoa' },
  { 231, 'bt','Bhutan' },
  { 222, 'ht','Haiti' },
  { 210, 'pt','Portugal' },
  { 219, 'mp','Northern Mariana Islands' },
  { 215, 'ch','Switzerland' },
  { 217, 'ms','Montserrat' },
  { 227, 'gb','United Kingdom' },
  { 214, 'io','British Indian Ocean Territory' },
  { 228, 'uk','United Kingdom' },
  { 224, 'ae','United Arab Emirates' },
  { 229, 'um','United States Minor Outlying Islands' },
  { 226, 'er','Eritrea' },
  { 230, 'us','United States' },
  { 225, 'vn','Vietnam' },
  { 207, 'at','Austria' },
  { 221, 'cf','Central African Republic' },
  { 218, 'mq','Martinique' },
  { 209, 'au','Australia' },
  { 236, 'pg','Papua New Guinea' },
  { 234, 'pe','Peru' },
  { 233, 'vu','Vanuatu' },
  { 237, 'ai','Anguilla' },
  { 235, 'bi','Burundi' },
  { 240, 'si','Slovenia' },
  { 238, 'sk','Slovak Republic' },
  { 241, 'bv','Bouvet Island' },
  { 239, 'lv','Latvia' },
  { 242, 'no','Norway' },
  { 243, 'tw','Taiwan' },
  { 246, 'va','Holy See (Vatican City State)' },
  { 244, 'ly','Libyan Arab Jamahiriya' },
  { 245, 'ke','Kenya' },
  { 248, 'tz','Tanzania' },
  { 247, 'br','Brazil' },
  { 1,   'sz','Swaziland' }
 ], RecDef);


JoinRecDef := 
            RECORD
RecDef;
string2         other;
            END;

JoinRecDef JoinTransform (RecDef l, RecDef r) := 
                TRANSFORM
                    SELF.other := r.cc;
                    SELF := l;
                END;
JoinRecDef SkipTransform (RecDef l, RecDef r) := 
                TRANSFORM
                    SELF.other := if (R.idx<50,skip,R.cc);
                    SELF := l;
                END;
            
NameTable1 := SORT(DISTRIBUTE(NameTable,HASH(name)+idx),cc) : INDEPENDENT;
NameTable2 := NameTable1;  


     Joined1    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT)); 
     Joined2    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT OUTER); 
     Joined3    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT ONLY); 
     Joined4    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),RIGHT OUTER); 
     Joined5    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),RIGHT ONLY); 
     Joined6    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),FULL OUTER); 
     Joined7    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),FULL ONLY); 
     Joined8    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT), ATMOST(1)); 
     Joined9    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT OUTER,ATMOST(1)); 
    Joined10    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT ONLY,ATMOST(1)); 
    Joined13    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT), LIMIT(2,SKIP)); 
    Joined14    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT OUTER,LIMIT(2,SKIP)); 
// ***codegen disallows combination of limit with only***
//    Joined15  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT ONLY,LIMIT(2,SKIP)); 
    Joined16    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),RIGHT OUTER,LIMIT(2,SKIP)); 
//    Joined17  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),RIGHT ONLY,LIMIT(2,SKIP)); 
    Joined18    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),FULL OUTER,LIMIT(2,SKIP)); 
//    Joined19  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),FULL ONLY,LIMIT(2,SKIP)); 
    Joined20    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT), KEEP(1)); 
    Joined21    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, JoinTransform(LEFT, RIGHT),LEFT OUTER,KEEP(1)); 
    Joined22    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT)); 
    Joined23    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT OUTER); 
    Joined24    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT ONLY); 
    Joined25    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),RIGHT OUTER); 
    Joined26    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),RIGHT ONLY); 
    Joined27    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),FULL OUTER); 
    Joined28    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),FULL ONLY); 
    Joined29    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT), ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined30    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT OUTER,ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined31    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT ONLY,ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined34    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT), LIMIT(2,SKIP)); 
    Joined35    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT OUTER,LIMIT(2,SKIP)); 
//    Joined36  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT ONLY,LIMIT(2,SKIP)); 
    Joined37    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),RIGHT OUTER,LIMIT(2,SKIP)); 
//    Joined38  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),RIGHT ONLY,LIMIT(2,SKIP)); 
    Joined39    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),FULL OUTER,LIMIT(2,SKIP)); 
//    Joined40  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),FULL ONLY,LIMIT(2,SKIP)); 
    Joined41    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT), KEEP(1)); 
    Joined42    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), JoinTransform(LEFT, RIGHT),LEFT OUTER,KEEP(1)); 
    Joined43    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT)); 
    Joined44    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT OUTER); 
    Joined45    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT ONLY); 
    Joined46    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),RIGHT OUTER); 
    Joined47    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),RIGHT ONLY); 
    Joined48    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),FULL OUTER); 
    Joined49    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),FULL ONLY); 
    Joined50    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT), ATMOST(1)); 
    Joined51    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT OUTER,ATMOST(1)); 
    Joined52    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT ONLY,ATMOST(1)); 
    Joined55    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT), LIMIT(2,SKIP)); 
    Joined56    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT OUTER,LIMIT(2,SKIP)); 
//    Joined57  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT ONLY,LIMIT(2,SKIP)); 
    Joined58    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),RIGHT OUTER,LIMIT(2,SKIP)); 
//    Joined59  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),RIGHT ONLY,LIMIT(2,SKIP)); 
    Joined60    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),FULL OUTER,LIMIT(2,SKIP)); 
//    Joined61  := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),FULL ONLY,LIMIT(2,SKIP)); 
    Joined62    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT), KEEP(1)); 
    Joined63    := join (NameTable1, NameTable2, LEFT.cc = RIGHT.cc, SkipTransform(LEFT, RIGHT),LEFT OUTER,KEEP(1)); 
    Joined64    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT)); 
    Joined65    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT OUTER); 
    Joined66    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT ONLY); 
    Joined67    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),RIGHT OUTER); 
    Joined68    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),RIGHT ONLY); 
    Joined69    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),FULL OUTER); 
    Joined70    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),FULL ONLY); 
    Joined71    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT), ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined72    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT OUTER,ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined73    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT ONLY,ATMOST(LEFT.cc = RIGHT.cc,1)); 
    Joined76    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT), LIMIT(2,SKIP)); 
    Joined77    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT OUTER,LIMIT(2,SKIP)); 
//    Joined78  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT ONLY,LIMIT(2,SKIP)); 
    Joined79    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),RIGHT OUTER,LIMIT(2,SKIP)); 
//    Joined80  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),RIGHT ONLY,LIMIT(2,SKIP)); 
    Joined81    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),FULL OUTER,LIMIT(2,SKIP)); 
//    Joined82  := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),FULL ONLY,LIMIT(2,SKIP)); 
    Joined83    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT), KEEP(1)); 
    Joined84    := join (NameTable1, NameTable2, (LEFT.cc = RIGHT.cc)AND(LEFT.idx<RIGHT.idx), SkipTransform(LEFT, RIGHT),LEFT OUTER,KEEP(1)); 

     output(sort(Joined1,idx,name,cc,other),NAMED('Joined1'),ALL);
     output(sort(Joined2,idx,name,cc,other),NAMED('Joined2'),ALL);
     output(sort(Joined3,idx,name,cc,other),NAMED('Joined3'),ALL);
     output(sort(Joined4,idx,name,cc,other),NAMED('Joined4'),ALL);
     output(sort(Joined5,idx,name,cc,other),NAMED('Joined5'),ALL);
     output(sort(Joined6,idx,name,cc,other),NAMED('Joined6'),ALL);
     output(sort(Joined7,idx,name,cc,other),NAMED('Joined7'),ALL);
     output(sort(Joined8,idx,name,cc,other),NAMED('Joined8'),ALL);
     output(sort(Joined9,idx,name,cc,other),NAMED('Joined9'),ALL);
    output(sort(Joined10,idx,name,cc,other),NAMED('Joined10'),ALL);
    output(sort(Joined13,idx,name,cc,other),NAMED('Joined13'),ALL);
    output(sort(Joined14,idx,name,cc,other),NAMED('Joined14'),ALL);
//    output(sort(Joined15,idx,name,cc,other),NAMED('Joined15'),ALL);
    output(sort(Joined16,idx,name,cc,other),NAMED('Joined16'),ALL);
//    output(sort(Joined17,idx,name,cc,other),NAMED('Joined17'),ALL);
    output(sort(Joined18,idx,name,cc,other),NAMED('Joined18'),ALL);
//    output(sort(Joined19,idx,name,cc,other),NAMED('Joined19'),ALL);
    output(count(Joined20),NAMED('Joined20'));          // keep not deterministic
    output(count(Joined21),NAMED('Joined21'));
    output(sort(Joined22,idx,name,cc,other),NAMED('Joined22'),ALL);
    output(sort(Joined23,idx,name,cc,other),NAMED('Joined23'),ALL);
    output(sort(Joined24,idx,name,cc,other),NAMED('Joined24'),ALL);
    output(sort(Joined25,idx,name,cc,other),NAMED('Joined25'),ALL);
    output(sort(Joined26,idx,name,cc,other),NAMED('Joined26'),ALL);
    output(sort(Joined27,idx,name,cc,other),NAMED('Joined27'),ALL);
    output(sort(Joined28,idx,name,cc,other),NAMED('Joined28'),ALL);
    output(sort(Joined29,idx,name,cc,other),NAMED('Joined29'),ALL);
    output(sort(Joined30,idx,name,cc,other),NAMED('Joined30'),ALL);
    output(sort(Joined31,idx,name,cc,other),NAMED('Joined31'),ALL);
    output(sort(Joined34,idx,name,cc,other),NAMED('Joined34'),ALL);
    output(sort(Joined35,idx,name,cc,other),NAMED('Joined35'),ALL);
//    output(sort(Joined36,idx,name,cc,other),NAMED('Joined36'),ALL);
    output(sort(Joined37,idx,name,cc,other),NAMED('Joined37'),ALL);
//    output(sort(Joined38,idx,name,cc,other),NAMED('Joined38'),ALL);
    output(sort(Joined39,idx,name,cc,other),NAMED('Joined39'),ALL);
//    output(sort(Joined40,idx,name,cc,other),NAMED('Joined40'),ALL);
    output(count(Joined41),NAMED('Joined41'));
    output(count(Joined42),NAMED('Joined42'));
    output(sort(Joined43,idx,name,cc,other),NAMED('Joined43'),ALL);
    output(sort(Joined44,idx,name,cc,other),NAMED('Joined44'),ALL);
    output(sort(Joined45,idx,name,cc,other),NAMED('Joined45'),ALL);
    output(sort(Joined46,idx,name,cc,other),NAMED('Joined46'),ALL);
    output(sort(Joined47,idx,name,cc,other),NAMED('Joined47'),ALL);
    output(sort(Joined48,idx,name,cc,other),NAMED('Joined48'),ALL);
    output(sort(Joined49,idx,name,cc,other),NAMED('Joined49'),ALL);
    output(sort(Joined50,idx,name,cc,other),NAMED('Joined50'),ALL);
    output(sort(Joined51,idx,name,cc,other),NAMED('Joined51'),ALL);
    output(sort(Joined52,idx,name,cc,other),NAMED('Joined52'),ALL);
    output(sort(Joined55,idx,name,cc,other),NAMED('Joined55'),ALL);
    output(sort(Joined56,idx,name,cc,other),NAMED('Joined56'),ALL);
//    output(sort(Joined57,idx,name,cc,other),NAMED('Joined57'),ALL);
    output(sort(Joined58,idx,name,cc,other),NAMED('Joined58'),ALL);
//    output(sort(Joined59,idx,name,cc,other),NAMED('Joined59'),ALL);
    output(sort(Joined60,idx,name,cc,other),NAMED('Joined60'),ALL);
//    output(sort(Joined61,idx,name,cc,other),NAMED('Joined61'),ALL);
    output(count(Joined62),NAMED('Joined62'));
    output(count(Joined63),NAMED('Joined63'));
    output(sort(Joined64,idx,name,cc,other),NAMED('Joined64'),ALL);
    output(sort(Joined65,idx,name,cc,other),NAMED('Joined65'),ALL);
    output(sort(Joined66,idx,name,cc,other),NAMED('Joined66'),ALL);
    output(sort(Joined67,idx,name,cc,other),NAMED('Joined67'),ALL);
    output(sort(Joined68,idx,name,cc,other),NAMED('Joined68'),ALL);
    output(sort(Joined69,idx,name,cc,other),NAMED('Joined69'),ALL);
    output(sort(Joined70,idx,name,cc,other),NAMED('Joined70'),ALL);
    output(sort(Joined71,idx,name,cc,other),NAMED('Joined71'),ALL);
    output(sort(Joined72,idx,name,cc,other),NAMED('Joined72'),ALL);
    output(sort(Joined73,idx,name,cc,other),NAMED('Joined73'),ALL);
    output(sort(Joined76,idx,name,cc,other),NAMED('Joined76'),ALL);
    output(sort(Joined77,idx,name,cc,other),NAMED('Joined77'),ALL);
//    output(sort(Joined78,idx,name,cc,other),NAMED('Joined78'),ALL);
    output(sort(Joined79,idx,name,cc,other),NAMED('Joined79'),ALL);
//    output(sort(Joined80,idx,name,cc,other),NAMED('Joined80'),ALL);
    output(sort(Joined81,idx,name,cc,other),NAMED('Joined81'),ALL);
//    output(sort(Joined82,idx,name,cc,other),NAMED('Joined82'),ALL);
    output(count(Joined83),NAMED('Joined83'));
    output(count(Joined84),NAMED('Joined84'));
