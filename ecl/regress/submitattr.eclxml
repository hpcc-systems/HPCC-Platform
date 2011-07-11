<Archive>
   <Query attributePath="TutorialLorraine.FetchPeopleByZipService"/>
   <Module key="tutoriallorraine" name="TutorialLorraine">
      <Attribute key="fetchpeoplebyzipservice" name="fetchpeoplebyzipservice"
                 sourcePath="TutorialLorraine\FetchPeopleByZipService.ecl">
#WORKUNIT(&apos;Name&apos;,&apos;FetchPeopleByZip&apos;) 
IMPORT TutorialLorraine; STRING10 ZipFilter := &apos;&apos;
         :STORED(&apos;ZIPValue&apos;); resultSet :=
         FETCH(TutorialLorraine.File_TutorialPerson,
         TutorialLorraine.IDX_PeopleByZIP(zip=ZipFilter), RIGHT.fpos);
         OUTPUT(resultset);&#32;
      </Attribute>
      <Attribute key="file_tutorialperson"
                  name="file_tutorialperson"
                  sourcePath="TutorialLorraine\File_TutorialPerson.ecl">
         IMPORT TutorialLorraine;
         EXPORT File_TutorialPerson :=
         DATASET(&apos;~tutorial::LC::TutorialPerson&apos;,
         {TutorialLorraine.Layout_People, UNSIGNED8 fpos {virtual(fileposition)}},
         THOR);&#13;&#10;
      </Attribute>
      <Attribute key="layout_people"
                 name="layout_people"
                 sourcePath="TutorialLorraine\Layout_People.ecl">
         EXPORT
         Layout_People := RECORD STRING15 FirstName; STRING25 LastName; STRING15
         MiddleName; STRING5 Zip; STRING42 Street; STRING20 City; STRING2 State;
         END;&#13;&#10;
      </Attribute>
      <Attribute key="idx_peoplebyzip"
name="idx_peoplebyzip" sourcePath="TutorialLorraine\IDX_PeopleByZip.ecl">
         IMPORT TutorialLorraine; export IDX_PeopleByZIP :=
         INDEX(TutorialLorraine.File_TutorialPerson,{zip,fpos},&apos;~tutorial::LC::PeopleByZipINDEX&apos;);
      </Attribute>
   </Module>
</Archive>