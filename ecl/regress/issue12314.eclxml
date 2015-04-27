<Archive legacyImport="0"
         legacyWhen="0">
 <Query attributePath="_local_directory_.temp"/>
 <OnWarning name="1006" value="ignore"/>
 <OnWarning name="12345" value="ignore"/>
 <Module key="_local_directory_" name="_local_directory_">
  <Attribute key="temp" name="temp" sourcePath="temp.ecl">
   case(0,&apos;default&apos;);&#10;&#10;&#10;
   #onwarning (12345, error); //overrides the setting in the archive
   #onwarning (54321, log);
  </Attribute>
 </Module>
</Archive>
