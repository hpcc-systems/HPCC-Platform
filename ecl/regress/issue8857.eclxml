<Archive build="community_3.8.6-1" legacyMode="0">
 <Query attributePath="_local_directory_.xyz"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="xyz" name="xyz" sourcePath="C:\xyz.ecl">
import red.source.yyy.xxx;
#stored(&apos;frequency&apos;, &apos;daily&apos;);

xxx.zzz.files_spray.web_user_curr_ds;
  </Attribute>
 </Module>
 <Module key="red" name="red"/>
 <Module key="red.source" name="red.source"/>
 <Module key="red.source.yyy" name="red.source.yyy"/>
 <Module key="red.source.yyy.xxx" name="red.source.yyy.xxx">
  <Attribute key="zzz" name="zzz" sourcePath="C:\development\source\yyy\xxx\zzz.ecl">
   import red.source.yyy.xxx;
import red.common;
import std;

EXPORT zzz := module

  shared COMMA_DELIM := &apos;,&apos;;
	shared PIPE_DELIM  := &apos;||&apos;;
	
	shared string FREQUENCY       :=  trim(STD.str.ToLowerCase(common.stored_frequency), all); 
	shared string NAME_SUFFIX     := if(frequency = &apos;daily&apos;, &apos;_prelim&apos;, &apos;_actual&apos;);  	
	
	shared String SPRAY_PREFIX		:= xxx.common.constants.spray_prefix;
	shared String STG_PREFIX			:= xxx.common.constants.stg_prefix;
	
  getCSVDelimiter := function
		return if (FREQUENCY = &apos;daily&apos;, COMMA_DELIM, PIPE_DELIM);
	end;

	getDailySprayDSName(String baseFileName, boolean prelimFlag=False) := function
		newBaseFileName := if (FREQUENCY = &apos;DAILY&apos;, baseFileName + &apos;_prelim&apos;, baseFileName);
		return  if (prelimFlag, SPRAY_PREFIX + &apos;::&apos; + newBaseFileName, SPRAY_PREFIX + &apos;::&apos; + baseFileName);
	end;

Export files_spray := module
		export web_user_curr_ds 		:= dataset(getDailySprayDSName(&apos;web_user_curr&apos;),xxx.layouts.web_user_curr,   csv(terminator(&apos;\n&apos;), separator(getCSVDelimiter), quote(&apos;&quot;&apos;)));
End;

	
End;
  </Attribute>
 </Module>
 <Module key="red.common" name="red.common">
  <Attribute key="stored_frequency" name="stored_frequency" sourcePath="C:\development\common\stored_frequency.ecl">
   EXPORT string stored_frequency := &apos;daily&apos; : stored(&apos;frequency&apos;);&#32;
  </Attribute>
 </Module>
 <Module key="std" name="std">
  <Attribute key="str" name="str" sourcePath="C:\Program Files (x86)\HPCC Systems\HPCC\bin\ver_3_6\ecllibrary\std\Str.ecl">
   /*##############################################################################
## Copyright (c) 2011 HPCC Systems®.  All rights reserved.
############################################################################## */

EXPORT Str := MODULE

/*
  Since this is primarily a wrapper for a plugin, all the definitions for this standard library
  module are included in a single file.  Generally I would expect them in individual files.
  */

IMPORT lib_stringlib;

/**
 * Returns the argument string with all upper case characters converted to lower case.
 * 
 * @param src           The string that is being converted.
 */

EXPORT STRING ToLowerCase(STRING src) := lib_stringlib.StringLib.StringToLowerCase(src);


END;&#13;&#10;
  </Attribute>
 </Module>
 <Module flags="5"
         fullname="C:\Program Files (x86)\HPCC Systems\HPCC\bin\ver_3_6\plugins\stringlib.dll"
         key="lib_stringlib"
         name="lib_stringlib"
         plugin="stringlib.dll"
         sourcePath="lib_stringlib"
         version="STRINGLIB 1.1.14">
  <Text>export StringLib := SERVICE
  string StringFilterOut(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilterOut&apos;; 
  string StringFilter(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilter&apos;; 
  string StringSubstituteOut(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubsOut&apos;; 
  string StringSubstitute(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubs&apos;; 
  string StringRepad(const string src, unsigned4 size) : c, pure,entrypoint=&apos;slStringRepad&apos;; 
  string StringTranslate(const string src, const string _within, const string _mapping) : c, pure,entrypoint=&apos;slStringTranslate&apos;; 
  unsigned integer4 StringFind(const string src, const string tofind, unsigned4 instance ) : c, pure,entrypoint=&apos;slStringFind&apos;; 
  unsigned integer4 StringUnboundedUnsafeFind(const string src, const string tofind ) : c, pure,entrypoint=&apos;slStringFind2&apos;; 
  unsigned integer4 StringFindCount(const string src, const string tofind) : c, pure,entrypoint=&apos;slStringFindCount&apos;; 
  unsigned integer4 EbcdicStringFind(const ebcdic string src, const ebcdic string tofind , unsigned4 instance ) : c,pure,entrypoint=&apos;slStringFind&apos;; 
  unsigned integer4 EbcdicStringUnboundedUnsafeFind(const ebcdic string src, const ebcdic string tofind ) : c,pure,entrypoint=&apos;slStringFind2&apos;; 
  string StringExtract(const string src, unsigned4 instance) : c,pure,entrypoint=&apos;slStringExtract&apos;; 
  string8 GetDateYYYYMMDD() : c,once,entrypoint=&apos;slGetDateYYYYMMDD2&apos;;
  varstring GetBuildInfo() : c,once,entrypoint=&apos;slGetBuildInfo&apos;;
  string Data2String(const data src) : c,pure,entrypoint=&apos;slData2String&apos;;
  data String2Data(const string src) : c,pure,entrypoint=&apos;slString2Data&apos;;
  string StringToLowerCase(const string src) : c,pure,entrypoint=&apos;slStringToLowerCase&apos;;
  string StringToUpperCase(const string src) : c,pure,entrypoint=&apos;slStringToUpperCase&apos;;
  string StringToProperCase(const string src) : c,pure,entrypoint=&apos;slStringToProperCase&apos;;
  string StringToCapitalCase(const string src) : c,pure,entrypoint=&apos;slStringToCapitalCase&apos;;
  string StringToTitleCase(const string src) : c,pure,entrypoint=&apos;slStringToTitleCase&apos;;
  integer4 StringCompareIgnoreCase(const string src1, string src2) : c,pure,entrypoint=&apos;slStringCompareIgnoreCase&apos;;
  string StringReverse(const string src) : c,pure,entrypoint=&apos;slStringReverse&apos;;
  string StringFindReplace(const string src, const string stok, const string rtok) : c,pure,entrypoint=&apos;slStringFindReplace&apos;;
  string StringCleanSpaces(const string src) : c,pure,entrypoint=&apos;slStringCleanSpaces&apos;; 
  boolean StringWildMatch(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringWildMatch&apos;; 
  boolean StringWildExactMatch(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringWildExactMatch&apos;; 
  boolean StringContains(const string src, const string _pattern, boolean _noCase) : c, pure,entrypoint=&apos;slStringContains&apos;; 
  string StringExtractMultiple(const string src, unsigned8 mask) : c,pure,entrypoint=&apos;slStringExtractMultiple&apos;; 
  unsigned integer4 EditDistance(const string l, const string r) : c, pure,entrypoint=&apos;slEditDistance&apos;; 
  boolean EditDistanceWithinRadius(const string l, const string r, unsigned4 radius) : c,pure,entrypoint=&apos;slEditDistanceWithinRadius&apos;; 
  unsigned integer4 EditDistanceV2(const string l, const string r) : c, pure,entrypoint=&apos;slEditDistanceV2&apos;; 
  boolean EditDistanceWithinRadiusV2(const string l, const string r, unsigned4 radius) : c,pure,entrypoint=&apos;slEditDistanceWithinRadiusV2&apos;; 
  string StringGetNthWord(const string src, unsigned4 n) : c, pure,entrypoint=&apos;slStringGetNthWord&apos;; 
  unsigned4 StringWordCount(const string src) : c, pure,entrypoint=&apos;slStringWordCount&apos;; 
  unsigned4 CountWords(const string src, const string _separator, BOOLEAN allow_blanks) : c, pure,entrypoint=&apos;slCountWords&apos;; 
  SET OF STRING SplitWords(const string src, const string _separator, BOOLEAN allow_blanks) : c, pure,entrypoint=&apos;slSplitWords&apos;; 
  STRING CombineWords(set of string src, const string _separator) : c, pure,entrypoint=&apos;slCombineWords&apos;; 
  UNSIGNED4 StringToDate(const string src, const varstring format) : c, pure,entrypoint=&apos;slStringToDate&apos;; 
  UNSIGNED4 MatchDate(const string src, set of varstring formats) : c, pure,entrypoint=&apos;slMatchDate&apos;; 
  STRING FormatDate(UNSIGNED4 date, const varstring format) : c, pure,entrypoint=&apos;slFormatDate&apos;; 
END;</Text>
 </Module>
 <Module key="red.source.yyy.xxx.common" name="red.source.yyy.xxx.common"/>
 <Module key="red.source.yyy.xxx.common.constants" name="red.source.yyy.xxx.common.constants">
  <Attribute key="spray_prefix" name="spray_prefix" sourcePath="C:\development\source\yyy\xxx\common\constants\spray_prefix.ecl">
   import red.dm;
import red.common;
import red.source.yyy.xxx;

Export spray_prefix 	 := case (common.stored_frequency,
																&apos;daily&apos; =&gt; &apos;~thor::red::spray::xxx::daily&apos; ,
																&apos;monthly&apos; =&gt; &apos;~thor::red::spray::xxx::monthly&apos;,
																 &apos;&apos;);&#13;&#10;
  </Attribute>
  <Attribute key="stg_prefix" name="stg_prefix" sourcePath="C:\development\source\yyy\xxx\common\constants\stg_prefix.ecl">
   Export stg_prefix      	:= &apos;~thor::red::stg::xxx&apos;;&#13;&#10;&#9;&#13;&#10;&#13;&#10;&#9;
  </Attribute>
 </Module>
 <Module key="red.dm" name="red.dm"/>
 <Module key="red.source.yyy.xxx.layouts" name="red.source.yyy.xxx.layouts">
  <Attribute key="web_user_curr" name="web_user_curr" sourcePath="C:\development\source\yyy\xxx\layouts\web_user_curr.ecl">
   export web_user_curr := record
integer 	web_user_id;
string10 	dw_start_dt;
string10 	dw_end_dt;
string11 	sub_acct_id;
string25 	user_signon_id;
string64 	web_user_last_name;
string64 	web_user_first_name;
string1 	web_user_mid_init;
string2 	web_user_type_cd;
string50 	web_user_type_descr;
string10 	web_id_cancel_dt;
string10 	create_dt;
string2 	actv_ind;
end;
  </Attribute>
 </Module>
</Archive>
