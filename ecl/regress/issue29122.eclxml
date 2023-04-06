<Archive build="community_8.10.12-1"
         eclVersion="8.10.12"
         legacyImport="0"
         legacyWhen="0">
 <Query attributePath="CodeDay23.BWR_BugTest"/>
 <Module key="codeday23" name="CodeDay23">
  <Attribute key="bwr_bugtest"
             name="BWR_BugTest"
             sourcePath="C:\Users\Public\Documents\HPCC Systems\ECL\My Files\CodeDay23\BWR_BugTest.ecl"
             ts="1678208915322594">
   trackrecordnew := RECORD
   string disc;
   string number;
   string tracktitle;
  END;

Layout := RECORD
  string name;
  string id;
  string rtype;
  string title;
  string genre;
  string releasedate;
  string formats;
  string label;
  string catalognumber;
  string producers;
  string guestmusicians;
  string description;
  DATASET(trackrecordnew) Tracks{maxcount(653)};
  string coversrc;
 END;

MozItemsDS := DATASET(&apos;~MIL::ItemsNormalized&apos;,Layout,THOR);


// OUTPUT(MozItemsDS.Tracks,{MozItemsDS.name,MozItemsDS.Tracks});  //This OUTPUT works

OUTPUT(MozItemsDS.Tracks,{MozItemsDS.id,MozItemsDS.Tracks});  //This OUTPUT fails&#13;&#10;&#13;&#10;
  </Attribute>
 </Module>
 <Option name="eclcc_compiler_version" value="8.10.12"/>
</Archive>
