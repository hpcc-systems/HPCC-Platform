<Archive>
    <!--

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
-->
    <Module name="zz_trentbuck">
  <Attribute name="mac_date">
   export mac_date(instring, outstring)  := macro
import ut;
#uniquename(newdate)
%newdate% := ut.AlphasOnly(instring);
outstring := %newdate%;
endmacro;
  </Attribute>
 </Module>
 <Module name="ut">
  <Attribute name="alphasonly">
   export AlphasOnly(string strIn) := stringlib.stringfilter(strIn, alphabet);
  </Attribute>
  <Attribute name="alphabet">
   export string26 alphabet := &apos;ABCDEFGHIJKLMNOPQRSTUVWXYZ&apos;;
  </Attribute>
 </Module>
 <Module fileCrc="3413467200"
         flags="13"
         name="lib_stringlib"
         plugin="plugins/libstringlib.so"
         rank="2"
         version="STRINGLIB 1.1.07">
  <Attribute name="stringlib">
   export StringLib := SERVICE
  string StringFilterOut(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilterOut&apos;;
  string StringFilter(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilter&apos;;
  string StringSubstituteOut(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubsOut&apos;;
  string StringSubstitute(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubs&apos;;
  string StringRepad(const string src, unsigned4 size) : c, pure,entrypoint=&apos;slStringRepad&apos;;
  unsigned integer4 StringFind(const string src, const string tofind, unsigned4 instance ) : c, pure,entrypoint=&apos;slStringFind&apos;, hole;
  unsigned integer4 StringFind2(const string src, const string tofind ) : c, pure,entrypoint=&apos;slStringFind2&apos;, hole;
  unsigned integer4 StringFindCount(const string src, const string tofind) : c, pure,entrypoint=&apos;slStringFindCount&apos;;
  unsigned integer4 EbcdicStringFind(const ebcdic string src, const ebcdic string tofind , unsigned4 instance ) : c,pure,entrypoint=&apos;slStringFind&apos;;
  unsigned integer4 EbcdicStringFind2(const ebcdic string src, const ebcdic string tofind ) : c,pure,entrypoint=&apos;slStringFind2&apos;;
  string StringExtract(const string src, unsigned4 instance) : c,pure,entrypoint=&apos;slStringExtract&apos;;
  string50 StringExtract50(const string src, unsigned4 instance) : c,pure,entrypoint=&apos;slStringExtract50&apos;, hole;
  string8 GetDateYYYYMMDD() : c,once,entrypoint=&apos;slGetDateYYYYMMDD2&apos;, hole;
  varstring GetBuildInfo() : c,once,entrypoint=&apos;slGetBuildInfo&apos;;
  string100 GetBuildInfo100() : c,once,entrypoint=&apos;slGetBuildInfo100&apos;, hole;
  string Data2String(const data src) : c,pure,entrypoint=&apos;slData2String&apos;;
  data String2Data(const string src) : c,pure,entrypoint=&apos;slString2Data&apos;;
  string StringToLowerCase(const string src) : c,pure,entrypoint=&apos;slStringToLowerCase&apos;;
  string StringToUpperCase(const string src) : c,pure,entrypoint=&apos;slStringToUpperCase&apos;;
  string StringToProperCase(const string src) : c,pure,entrypoint=&apos;slStringToProperCase&apos;;
  string80 StringToLowerCase80(const string src) : c,pure,entrypoint=&apos;slStringToLowerCase80&apos;, hole;
  string80 StringToUpperCase80(const string src) : c,pure,entrypoint=&apos;slStringToUpperCase80&apos;, hole;
  integer4 StringCompareIgnoreCase(const string src1, string src2) : c,pure,entrypoint=&apos;slStringCompareIgnoreCase&apos;, hole;
  string StringReverse(const string src) : c,pure,entrypoint=&apos;slStringReverse&apos;;
  string StringFindReplace(const string src, const string stok, const string rtok) : c,pure,entrypoint=&apos;slStringFindReplace&apos;;
  string80 StringFindReplace80(const string src, const string stok, const string rtok) : c,pure,entrypoint=&apos;slStringFindReplace80&apos;, hole;
  string25 StringCleanSpaces25(const string src) : c,pure,entrypoint=&apos;slStringCleanSpaces25&apos;, hole;
  string80 StringCleanSpaces80(const string src) : c,pure,entrypoint=&apos;slStringCleanSpaces80&apos;, hole;
  string StringCleanSpaces(const string src) : c,pure,entrypoint=&apos;slStringCleanSpaces&apos;;
  boolean StringWildMatch(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringWildMatch&apos;, hole;
  boolean StringContains(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringContains&apos;, hole;
  string StringExtractMultiple(const string src, unsigned8 mask) : c,pure,entrypoint=&apos;slStringExtractMultiple&apos;;
  unsigned integer4 EditDistance(const string l, const string r) : c, pure,entrypoint=&apos;slEditDistance&apos;;
  boolean EditDistanceWithinRadius(const string l, const string r, unsigned4 radius) : c,pure,entrypoint=&apos;slEditDistanceWithinRadius&apos;;
END;
  </Attribute>
 </Module>
 <Query>
    import zz_trentBuck;
    import ut;
rec := record
    string10 anystring;
end;

ds := dataset([{&apos;ABC123DEF&apos;}],rec);
rec anytrans(rec l) := transform
            zz_trentBuck.mac_date(l.anystring,outstring)
            self.anystring := outstring; end; rs := project(ds,anytrans(left)); output(rs);&#13;&#10;&#13;&#10;</Query>
</Archive>
