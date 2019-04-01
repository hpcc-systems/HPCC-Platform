<Archive build="internal_7.2.1-closedown0"
         eclVersion="7.2.1"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="_local_directory_.temp"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="temp"
             name="temp"
             sourcePath="/home/gavin/dev/hpcc/ecl/regress/temp.ecl"
             ts="1553856593000000">
   &#32;import Std.Str;

output(Str.toUpperCase(&apos;zz&apos;));&#10;
  </Attribute>
 </Module>
 <Module key="std" name="std">
  <Attribute key="str"
             name="Str"
             sourcePath="/home/gavin/dev/hpcc/ecllibrary/std/Str.ecl"
             ts="1545228834000000">
EXPORT Str := MODULE


/*
  Since this is primarily a wrapper for a plugin, all the definitions for this standard library
  module are included in a single file.  Generally I would expect them in individual files.
  */

IMPORT lib_stringlib;

/**
 * Return the argument string with all lower case characters converted to upper case.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToUpperCase(STRING src, unsigned newFromParam = 0, unsigned newToParam = -1) := lib_stringlib.StringLib.StringToUpperCase(src, newFromParam, unsigned newToParam);

END;&#10;
  </Attribute>
 </Module>
 <Module flags="5"
         fullname="/home/gavin/buildr/RelWithDebInfo/libs/libstringlib.so"
         key="lib_stringlib"
         name="lib_stringlib"
         plugin="libstringlib.so"
         sourcePath="lib_stringlib"
         ts="1553256631000000"
         version="STRINGLIB 1.1.14">
  <Text>export StringLib := SERVICE:fold
  string StringToUpperCase(const string src) : c,pure,entrypoint=&apos;slStringToUpperCase&apos;;
END;</Text>
 </Module>
</Archive>
