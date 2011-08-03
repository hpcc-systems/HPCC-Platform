<Archive>
 <Module key="imdb" name="IMDB">
  <Attribute key="kevinbaconnumbersets" name="kevinbaconnumbersets" sourcePath="IMDB\KevinBaconNumberSets.ecl">
   /* ******************************************************************************
    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.


 ATTRIBUTE PURPOSE:
 Produce a series of sets for Actors and Movies that are : distance-0
 away (KBacons Direct movies ), distance-2 Away KBacon&apos;s Costars Movies ,
 distance-3 away - Movies of Costars of Costars etc all the way upto level 7

 The nested attributes below are shown here together for the benefit of the reader.

 Notes on variable naming convention used for costars and movies
 KBMovies               :  Movies  Kevin Bacon Worked in    (distance 0)
 KBCoStars              :  Stars who worked in KBMovies      (distance 1)
 KBCoStarMovies         :  Movies worked in by KBCoStars
                             except KBMovies   (distance 1)
 KBCo2Stars             :  Stars(Actors) who worked in KBCoStarMovies (distance 2)
 KBCo2StarMovies        :  Movies worked  in by KBCo2Stars
                             except KBCoStarMovies    (distance 2)
 KBCo3Stars             :  Stars(Actors) who worked in KBCo2StarMovies (distance 3)
 KBCo3StarMovies        :  Movies worked  in by KBCo3Stars
                             except KBCo2StarMovies   (distance 3)
etc..
******************************************************************************* */

IMPORT Std;
IMPORT IMDB;

EXPORT KevinBaconNumberSets := MODULE
  // Constructing a proper name match function is an art within itself
  // For simplicity we will define a name as matching if both first and last name are found within the string

  NameMatch(string full_name, string fname,string lname) :=
    Std.Str.Find(full_name,fname,1) &gt; 0 AND
    Std.Str.Find(full_name,lname,1) &gt; 0;

  //------ Get KBacon Movies
  AllKBEntries := IMDB.ActorsInMovies(NameMatch(actor,&apos;Kevin&apos;,&apos;Bacon&apos;));
  EXPORT KBMovies := DEDUP(AllKBEntries, movie, ALL); // Each movie should ONLY occur once

  //------ Get KBacon CoStars
  CoStars := IMDB.ActorsInMovies(Movie IN SET(KBMovies,Movie));
  EXPORT KBCoStars := DEDUP( CoStars(actor&lt;&gt;&apos;Kevin Bacon&apos;), actor, ALL);

  //------ Get KBacon Costars&apos; Movies
  // CSM = First find all of the movies that a KBCoStar has been in

  CSM := DEDUP(JOIN(IMDB.ActorsInMovies,KBCoStars, LEFT.actor=RIGHT.actor,
                    TRANSFORM(LEFT), LOOKUP),
               movie,ALL);

  // Now we need to remove all of those that KB was in himself
  // We can use a set; KB has not been in (quite!) that many movies

  EXPORT KBCoStarMovies := CSM(movie NOT IN SET(KBMovies,movie));

  //------ Bacon # 2 Actors
  // To be a Co2Star of Kevin Bacon you must have appeared in a movie with a CoStar of Kevin Bacon
  // This corresponds to having a Bacon number of 2
  // We are now getting towards the expensive part of the process
  KBCo2S := DEDUP(JOIN(IMDB.ActorsInMovies, KBCoStarMovies, LEFT.movie=RIGHT.movie,
                       TRANSFORM(LEFT), LOOKUP),
                  actor, ALL);

  // KCCo2S = ALL Actors appearing in Movies of KBacon&apos;s CoActors
  // The above is all the people in the movies; but some will have been co-stars of KB directly - these must be removed
  // The LEFT ONLY join removes items in one list from another

  EXPORT KBCo2Stars := JOIN(KBCo2S, KBCoStars, LEFT.actor=RIGHT.actor, TRANSFORM(LEFT), LEFT ONLY);

  //------- bacon # 2 Movies
  // Co2SM = what movies have all the Co2Stars been in?
  Co2SM := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo2Stars, LEFT.actor=RIGHT.actor,
                      TRANSFORM(LEFT), LOOKUP),
                 movie, ALL);
  // Co2SM = ALL Movies KBCo2Stars have been in
  // Of course some of these movies will have CoStars in too and thus will already have been listed.
  // Note this list will not contain any Kevin Bacon movies OR the movie would have been reached earlier!

  Export KBCo2StarMovies := JOIN(Co2SM, KBCoStarMovies, LEFT.movie=RIGHT.movie,
                                 TRANSFORM(LEFT),LEFT ONLY);

  //------ bacon #3 Actors
  // Find people with a Bacon number of 3
  // This code is very similar to KBCo2Stars; one might be tempted to common up into a function or macro
  // However it is worth looking at the attribute counts first; we may be down to a small enough set that we can start
  // using in memory functions (such as SET) again.

  KBCo3S := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo2StarMovies, LEFT.movie=RIGHT.movie,
                       TRANSFORM(LEFT), LOOKUP),
                  actor, ALL);

  // KBCo3S = ALL CoStars  in KBCo2Star Movies
  // The above is all the people in the movies; but some will have been co2stars of KB directly - these must be removed
  // The LEFT ONLY join removes items in one list from another
  // There should not be any direct CoStars in this list (or the movie would have been a CoStarMovie not a CoCoStarMovie)

  EXPORT KBCo3Stars := JOIN(KBCo3S, KBCo2Stars, LEFT.actor=RIGHT.actor,
                            TRANSFORM(LEFT),LEFT ONLY);

  //----- bacon #3 Movies
  // So what movies have all the KBCo3Stars been in?

  Co3SM := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo3Stars, LEFT.actor=RIGHT.actor,
                      TRANSFORM(LEFT), LOOKUP),
                 movie, ALL);

  // Co3SM = ALL Movies KBCo3Stars have been in
  // Of course some of these movies will have KBCo2Stars in too and thus will already have been listed.
  // Note We ONLY have to remove one level back from the list; previous levels cannot be reach by definition

  EXPORT KBCo3StarMovies := JOIN(Co3SM, KBCo2StarMovies, LEFT.movie=RIGHT.movie,
                                 TRANSFORM(LEFT),LEFT ONLY);

  //------bacon #4 Actors
  KBCo4S := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo3StarMovies, LEFT.movie=RIGHT.movie,
                       TRANSFORM(LEFT), LOOKUP),
                  actor, ALL);

  EXPORT KBCo4Stars := JOIN(KBCo4S, KBCo3Stars, LEFT.actor=RIGHT.actor,
                            TRANSFORM(LEFT),LEFT ONLY);

  //----- bacon #4 Movies
  // So what movies have all the Co4Stars been in?

  Co4SM := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo4Stars, LEFT.actor=RIGHT.actor,
                      TRANSFORM(LEFT), LOOKUP),
                 movie, ALL);

  // Co4SM = ALL Movies KBCo4Stars have been in
  // Of course some of these movies will have Co3Stars in too and thus will already have been listed.
  // Note We ONLY have to remove one level back from the list; previous levels cannot be reach by definition

  EXPORT KBCo4StarMovies := JOIN(Co4SM, KBCo3StarMovies, LEFT.movie=RIGHT.movie,
                                 TRANSFORM(LEFT),LEFT ONLY);

  //----- bacon #5 Stars
  KBCo5S := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo4StarMovies, LEFT.movie=RIGHT.movie,
                       TRANSFORM(LEFT), LOOKUP),
                  actor, ALL);

  EXPORT KBCo5Stars := JOIN(KBCo5S, KBCo4Stars, LEFT.actor=RIGHT.actor, TRANSFORM(LEFT),LEFT ONLY);

//----- bacon #5 Movies
  Co5SM := DEDUP(JOIN(IMDB.ActorsInMovies, KBCo5Stars, LEFT.actor=RIGHT.actor,
                      TRANSFORM(LEFT), LOOKUP),
                 movie,ALL);

  EXPORT KBCo5StarMovies := JOIN(Co5SM, KBCo4StarMovies, LEFT.movie=RIGHT.movie,
                                 TRANSFORM(LEFT),LEFT ONLY);

  //----- bacon #6 Stars
  // Find people with a Bacon number of 6
  // KBCo5 is getting small again - can move back down to the SET?

  KBCo6S  := DEDUP(IMDB.ActorsInMovies(movie IN SET(KBCo5StarMovies, movie)),
                   actor, ALL);

  EXPORT KBCo6Stars := JOIN(KBCo6S, KBCo5Stars, LEFT.actor=RIGHT.actor,
                            TRANSFORM(LEFT),LEFT ONLY);

  //----- bacon #6 Movies
  Co6SM := DEDUP(IMDB.ActorsInMovies(actor IN SET(KBCo6Stars, actor)), movie, ALL);

  EXPORT KBCo6StarMovies := Co6SM(movie NOT IN SET(KBCo5StarMovies, movie));

  //----- bacon #7 Movies
  // Find people with a Bacon number of 7
  KBCo7S := DEDUP(IMDB.ActorsInMovies(movie IN SET(KBCo6StarMovies,movie)), actor, ALL);
  EXPORT KBCo7Stars := KBCo7S(actor NOT IN SET(KBCo6Stars, actor));

  //----- We just have to count them all !! (How many holes in Albert Hall?)
  EXPORT doCounts := PARALLEL(
    OUTPUT(COUNT(KBMovies), NAMED(&apos;KBMovies&apos;)),
    OUTPUT(COUNT(KBCoStars), NAMED(&apos;KBCoStars&apos;)),
    OUTPUT(COUNT(KBCoStarMovies), NAMED(&apos;KBCoStarMovies&apos;)),
    OUTPUT(COUNT(KBCo2Stars), NAMED(&apos;KBCo2Stars&apos;)),
    OUTPUT(COUNT(KBCo2StarMovies), NAMED(&apos;KBCo2StarMovies&apos;)),
    OUTPUT(COUNT(KBCo3Stars), NAMED(&apos;KBCo3Stars&apos;)),
    OUTPUT(COUNT(KBCo3StarMovies), NAMED(&apos;KBCo3StarMovies&apos;)),
    OUTPUT(COUNT(KBCo4Stars), NAMED(&apos;KBCo4Stars&apos;)),
    OUTPUT(COUNT(KBCo4StarMovies), NAMED(&apos;KBCo4StarMovies&apos;)),
    OUTPUT(COUNT(KBCo5Stars), NAMED(&apos;KBCo5Stars&apos;)),
    OUTPUT(COUNT(KBCo5StarMovies), NAMED(&apos;KBCo5StarMovies&apos;)),
    OUTPUT(COUNT(KBCo6Stars), NAMED(&apos;KBCo6Stars&apos;)),
    OUTPUT(COUNT(KBCo6StarMovies), NAMED(&apos;KBCo6StarMovies&apos;)),
    OUTPUT(COUNT(KBCo7Stars), NAMED(&apos;KBCo7Stars&apos;)),
    OUTPUT(KBCo7Stars)
  );

END;&#13;&#10;&#13;&#10;
  </Attribute>
  <Attribute key="actorsinmovies" name="actorsinmovies" sourcePath="IMDB\ActorsInMovies.ecl">
   /* ******************************************************************************
    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it AND/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.
******************************************************************************* */

/**
  * Produce a slimmed down version of the IMDB actor AND actress files to
  * permit more efficient join operations.
  * Filter out the movie records we do not want in building our KBacon Number sets.
  *
  */

IMPORT IMDB,Std;

// Filter out TV movies, Videos AND some documentary type collections
ds_IMDB := IMDB.FileActors(actorname!=&apos;&apos; AND moviename != &apos;&apos; AND
                           Std.Str.Find(moviename,&apos;Boffo&apos;,1) = 0 AND
                           Std.Str.Find(moviename,&apos;Slasher Film&apos;,1) = 0 AND
                           movie_type != &apos;Video&apos; AND isTVseries = &apos;N&apos; AND
                           movie_type != &apos;For TV&apos;);

//Slim the records down to bare essentials for searching AND joining
slim_IMDB_rec := RECORD
  string50  actor;
  string150 movie;
END;

slim_IMDB_rec slim_it(ds_IMDB L):= TRANSFORM
  SELF.actor := Std.Str.FindReplace(L.actorname,&apos;(I)&apos;,&apos;&apos;);
  SELF.movie := L.moviename;;
END;

IMDB_names := PROJECT(ds_IMDB, slim_it(LEFT));

export ActorsInMovies := IMDB_Names;&#13;&#10;
  </Attribute>
  <Attribute key="fileactors" name="fileactors" sourcePath="IMDB\FileActors.ecl">
   /* ******************************************************************************
    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.

 ATTRIBUTE PURPOSE:
 1. Process and Transform the raw sprayed files for actor and actresses into formats
    more suitable for subsequent processing &amp; analysis
 2. This will strip out the headers and footers from the files,combine them into
    one file, eliminate some&apos;dirty&apos;  or incompete records, and parse the free form
        text lines into formatted records.
******************************************************************************* */

IMPORT IMDB,Std;

base:= IF(Std.System.Job.platform()=&apos;standalone&apos;, &apos;&apos;, &apos;~foreign::10.121.159.48::thor::in::IMDB::&apos;) : GLOBAL;

raw_rec:= RECORD
    STRING rawtext {maxlength(1024)};
    STRING6 gender;
END;

// HEADING is to strip the pre-amble ...
// A different solution would be to look for some kind of data element in the pre-able at filter everthing before that ...
// This would be safer if the length of the pre-amble changes
// Of course if the last line of the pre-amble changed we would still be dead!
// QUOTE is required as some of the &apos;quotes&apos; in the text are unbalanced
// SEPARATOR is used to allow the tabs through unscathed
ds_raw_male  := DATASET(base+&apos;actors.list&apos;,raw_rec,CSV(HEADING(239),SEPARATOR(&apos;|&apos;),QUOTE([]) ));

ds_raw_female:= DATASET(base+&apos;actresses.list&apos;,raw_rec,CSV(HEADING(241),SEPARATOR(&apos;|&apos;),QUOTE([])));

// Here we are going to remove multiple tab sequences to produce one single tab separator
// makes downstream process a lot cleaner
// We are also going to join actors and actresses together but retain gender information in a seperate field.
raw_rec StripDoubleTab(raw_rec le, string6 G) := TRANSFORM
  SELF.rawtext := Std.Str.FindReplace(Std.Str.FindReplace(le.rawtext,&apos;\t\t\t&apos;,&apos;\t&apos;),&apos;\t\t&apos;,&apos;\t&apos;);
  SELF.Gender  := G;
END;

raw_male := PROJECT(ds_raw_male(rawtext&lt;&gt;&apos;&apos;),StripDoubleTab(LEFT,&apos;MALE&apos;));
raw_female := PROJECT(ds_raw_female(rawtext&lt;&gt;&apos;&apos;),StripDoubleTab(LEFT,&apos;FEMALE&apos;));


// the record structure we want to transform to -- will be used in Transform below
layout_actor_rec:= RECORD
  STRING actorname {maxlength(2048)};
  STRING filmname {maxlength(2048)};
  STRING6 Gender;
END;

// These needs to be a function as the iterate is used to process records in sequence
// As the male and female are separate they need to be processed separately
into_three(dataset(raw_rec) ds_in) := FUNCTION

  // The incoming lines are in one of two formats, either:
  // a) Actorname \t MovieTitle
  // b) MovieTitle
  // The purpose of this transform is to move both into the same format

  layout_actor_rec prepActorFilmNames(ds_in L) := TRANSFORM
    UNSIGNED2 TabPos := Std.Str.Find(L.RawText,&apos;\t&apos;,1); // Variables are local to transform to save typing
    BOOLEAN HasName  := TabPos &gt; 0;
    SELF.actorname   := IF ( HasName, IMDB.CleanActor(L.RawText[1..TabPos-1]), &apos;&apos; );
    SELF.filmname    := if ( HasName, L.RawText[TabPos+1..],L.RawText );
    SELF             := L; // Only moving Gender presently;
  END;

  //PROJECT the input Dataset using the prepActorFilmNames transform
  prep_ds1           := PROJECT(ds_in, prepActorFilmNames(LEFT));

  //For those records that do not have an actorname it is possible to move it from the previous record
  // In the transform for an iterate the R is the &apos;current&apos; record
  // L is the &apos;last&apos; one so that we can copy any data we wish
  layout_actor_rec FillActor(prep_ds1 L, prep_ds1 R) := TRANSFORM
    SELF.actorname := IF( r.actorname = &apos;&apos;,L.ActorName,R.ActorName );
    SELF           := R;
  END;

  // Iterate the prep dataset using the FillActor Transform to get the structure we want
  RETURN ITERATE(prep_ds1,FillActor(LEFT,RIGHT));
END;

// The iterate is now done so male and female can be safely process as one database
ThreeColumns := into_three(raw_male)+into_three(raw_female);

// Now we have three fields defined with want to dig into the filmname format to parse out some of the extra information into fields
// We have place our target layout in a seperate attribute; it could be inline but for people using this attribute it is nice
// to be able to see the data format without having to wade through all the low level code.

// Here we encapsulate the code to pull data out from between boundary markers such as (){} etc
FindWithin(string Src,String ToFind,String ToEnd) :=
   IF ( Std.Str.Find(Src,ToFind,1) &gt; 0,
        Src[Std.Str.Find(Src,ToFind,1)+length(ToFind)..Std.Str.Find(Src,ToEnd,1)-1],
        &apos;&apos; );

// This transform plucks the data field by field out of the &apos;filmname&apos; which has &apos;magic symbols&apos; in the 3 column format
IMDB.LayoutActors tActors(ThreeColumns L) := TRANSFORM

  // TV series are enclosed within &quot;&quot;
  self.isTVSeries   := IF( L.filmname[1] = &apos;&quot;&apos;, &apos;Y&apos;,&apos;N&apos; );

  // The movie name is the first thing on the line and will be followed by the year in ()
  // If it is a TV program it will be in &quot;
  self.moviename    := IF (L.filmname[1]=&apos;&quot;&apos;, L.filmname[2..Std.Str.Find(L.filmname,&apos;&quot;&apos;,2)-1],L.filmname[1..Std.Str.Find(L.filmname,&apos;(&apos;,1)-1]);
  self.movie_type   := MAP ( Std.Str.Find(L.filmname,&apos;(TV)&apos;,1) &gt; 0 =&gt; &apos;For TV&apos;,
                             Std.Str.Find(L.filmname,&apos;(V)&apos;,1) &gt; 0 =&gt; &apos;Video&apos;,
                             &apos;&apos;);
  self.year         := FindWithin(L.filmname,&apos;(&apos;,&apos;)&apos;);
  self.rolename     := FindWithin(L.filmname,&apos;[&apos;,&apos;]&apos;);
  self.credit_pos   := (integer2)FindWithin(L.filmname,&apos;&lt;&apos;,&apos;&gt;&apos;);
  self.episode_name := FindWithin(L.filmname,&apos;{&apos;,&apos;}&apos;);
  self.episode_num  := FindWithin(L.filmname,&apos;(#&apos;,&apos;)&apos;);
  SELF              := L; // Copy across everything but movie name
END;

// The year filter is a quick fix of tackling the actual problem  which is
// that we have a &apos;tail&apos; on the file which contains &apos;junk&apos; as far as the
// database is concerned..This solution filters out all date-less entries;
// some of these are NOT in the tail - but all of them are non valid anyway.

EXPORT FileActors := PROJECT(ThreeColumns,tActors(LEFT))(year&lt;&gt;&apos;&apos;) : persist(&apos;~temp::file_actorsv2&apos;);&#13;&#10;
  </Attribute>
  <Attribute key="cleanactor" name="cleanactor" sourcePath="IMDB\CleanActor.ecl">
   /* ******************************************************************************
    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.
 ***************************************************************************** */

IMPORT Std;

EXPORT STRING CleanActor(STRING infld) := FUNCTION
  //this can be refined later
  s1 := Std.Str.FindReplace(infld, &apos;\&apos;&apos;,&apos;&apos;); // replace apostrophe
  s2 := Std.Str.FindReplace(s1, &apos;\t&apos;,&apos;&apos;);    //replace tabs
  s3 := Std.Str.FindReplace(s2, &apos;----&apos;,&apos;&apos;);  // replace multiple -----
  return TRIM(s3, LEFT, RIGHT);
END;&#13;&#10;
  </Attribute>
  <Attribute key="layoutactors" name="layoutactors" sourcePath="IMDB\LayoutActors.ecl">
   /* ******************************************************************************
    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.

 ATTRIBUTE PURPOSE:
 1. This is to specify the record layout of the IMDB actors file
    The raw input file is transformed into this record structure by the
        attribute: file_actors.
        The reason for separating it out-- is to reuse it in other attributes
        to write queries or analytics or any other processing
******************************************************************************* */

EXPORT LayoutActors:= RECORD
  STRING100 actorname;
  STRING100 moviename;
  STRING6   movie_type;   // values (TV)= made for TV  or (V)= Video Clip?
  STRING1   isTVseries;   // Y|N Flag
  STRING4   year;         // year movie was made
  STRING50  rolename;     // actors character role in the movie
  INTEGER2  credit_pos;   // Position # in credits

  //the following only have values if movie is a TV series
  STRING100 episode_name;
  STRING4   episode_num;  // 6.3 means season 6 , episode 3
END;
  </Attribute>
 </Module>
 <Module key="std" name="std">
  <Attribute key="str" name="str" sourcePath="std\Str.ecl">
   /*##############################################################################

    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.
############################################################################## */

EXPORT Str := MODULE

/*
  Since this is primarily a wrapper for a plugin, all the definitions for this standard library
  module are included in a single file.  Generally I would expect them in individual files.
  */

IMPORT * FROM lib_stringlib;

/*
 * Compares the two strings case insensitively.  Returns a negative integer, zero, or a positive integer according to
 * whether the first string is less than, equal to, or greater than the second.
 *
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @see                 Str.EqualIgnoreCase
 */

EXPORT INTEGER4 CompareIgnoreCase(STRING src1, STRING src2) :=
  StringLib.StringCompareIgnoreCase(src1, src2);

/*
 * Tests whether the two strings are identical ignoring differences in case.
 *
 * @param src1          The first string to be compared.
 * @param src2          The second string to be compared.
 * @see                 Str.CompareIgnoreCase
 */

EXPORT BOOLEAN EqualIgnoreCase(STRING src1, STRING src2) := CompareIgnoreCase(src1, src2) = 0;

/*
 * Returns the character position of the nth match of the search string with the first string.
 * If no match is found the attribute returns 0.
 * If an instance is omitted the position of the first instance is returned.
 *
 * @param src           The string that is searched
 * @param sought        The string being sought.
 * @param instance      Which match instance are we interested in?
 */

EXPORT UNSIGNED4 Find(STRING src, STRING sought, UNSIGNED4 instance = 1) :=
  StringLib.StringFind(src, sought, instance);

/*
 * Returns the number of occurences of the second string within the first string.
 *
 * @param src           The string that is searched
 * @param sought        The string being sought.
 */

EXPORT UNSIGNED4 FindCount(STRING src, STRING sought) := StringLib.StringFindCount(src, sought);

/*
 * Tests if the search string matches the pattern.
 * The pattern can contain wildcards &apos;?&apos; (single character) and &apos;*&apos; (multiple character).
 *
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @ignore_case         Whether to ignore differences in case between characters
 */

EXPORT BOOLEAN WildMatch(STRING src, STRING _pattern, BOOLEAN ignore_case) :=
  StringLib.StringWildExactMatch(src, _pattern, ignore_case);

/*
 * Tests if the search string contains each of the characters in the pattern.
 * If the pattern contains duplicate characters those characters will match once for each occurence in the pattern.
 *
 * @param src           The string that is being tested.
 * @param pattern       The pattern to match against.
 * @ignore_case         Whether to ignore differences in case between characters
 */

EXPORT BOOLEAN Contains(STRING src, STRING _pattern, BOOLEAN ignore_case) :=
  StringLib.StringContains(src, _pattern, ignore_case);

/*
 * Returns the first string with all characters within the second string removed.
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be excluded.
 * @see                 Str.Filter
 */

EXPORT STRING FilterOut(STRING src, STRING filter) := StringLib.StringFilterOut(src, filter);

/*
 * Returns the first string with all characters not within the second string removed.
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @see                 Str.FilterOut
 */

EXPORT STRING Filter(STRING src, STRING filter) := StringLib.StringFilter(src, filter);

/*
 * Returns the source string with the replacement character substituted for all characters included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Str.SubstituteExcluded
 */

EXPORT STRING SubstituteIncluded(STRING src, STRING filter, STRING replace_char) :=
  StringLib.StringSubstituteOut(src, filter, replace_char);

/*
 * Returns the source string with the replacement character substituted for all characters not included in the
 * filter string.
 * MORE: Should this be a general string substitution?
 *
 * @param src           The string that is being tested.
 * @param filter        The string containing the set of characters to be included.
 * @param replace_char  The character to be substituted into the result.
 * @see                 Std.Str.SubstituteIncluded
 */

EXPORT STRING SubstituteExcluded(STRING src, STRING filter, STRING replace_char) :=
  StringLib.StringSubstitute(src, filter, replace_char);

/*
 * Returns the argument string with all upper case characters converted to lower case.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToLowerCase(STRING src) := StringLib.StringToLowerCase(src);

/*
 * Return the argument string with all lower case characters converted to upper case.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToUpperCase(STRING src) := StringLib.StringToUpperCase(src);

/*
 * Returns the argument string with the first letter of each word in upper case and all other
 * letters left as-is.
 * A contiguous sequence of alphabetic characters is treated as a word.
 *
 * @param src           The string that is being converted.
 */

EXPORT STRING ToCapitalCase(STRING src) := StringLib.StringToCapitalCase(src);

/*
 * Returns the argument string with all characters in reverse order.
 * Note the argument is not TRIMMED before it is reversed.
 *
 * @param src           The string that is being reversed.
 */

EXPORT STRING Reverse(STRING src) := StringLib.StringReverse(src);

/*
 * Returns the source string with the replacement string substituted for all instances of the search string.
 *
 * @param src           The string that is being transformed.
 * @param sought        The string to be replaced.
 * @param replacement   The string to be substituted into the result.
 */

EXPORT STRING FindReplace(STRING src, STRING sought, STRING replacement) :=
  StringLib.StringFindReplace(src, sought, replacement);

/*
 * Returns the nth element from a comma separated string.
 *
 * @param src           The string containing the comma separated list.
 * @param instance      Which item to select from the list.
 */

EXPORT STRING Extract(STRING src, UNSIGNED4 instance) := StringLib.StringExtract(src, instance);

/*
 * Returns the source string with all instances of multiple adjacent space characters (2 or more spaces together)
 * reduced to a single space character.  Leading and trailing spaces are removed, and tab characters are converted
 * to spaces.
 *
 * @param src           The string to be cleaned.
 */

EXPORT STRING CleanSpaces(STRING src) := StringLib.StringCleanSpaces(src);

/*
 * Returns true if the prefix string matches the leading characters in the source string.  Trailing spaces are
 * stripped from the prefix before matching.
 * // x.myString.StartsWith(&apos;x&apos;) as an alternative syntax would be even better
 *
 * @param src           The string being searched in.
 * @param prefix        The prefix to search for.
 */

EXPORT BOOLEAN StartsWith(STRING src, STRING prefix) := src[1..LENGTH(TRIM(prefix))]=prefix;

/*
 * Returns true if the suffix string matches the trailing characters in the source string.  Trailing spaces are
 * stripped from both strings before matching.
 *
 * @param src           The string being searched in.
 * @param suffix        The prefix to search for.
 */
export BOOLEAN EndsWith(STRING src, STRING suffix) := src[LENGTH(TRIM(src))-LENGTH(TRIM(suffix))+1..]=suffix;


/*
 * Removes the suffix from the search string, if present, and returns the result.  Trailing spaces are
 * stripped from both strings before matching.
 *
 * @param src           The string being searched in.
 * @param suffix        The prefix to search for.
 */
export STRING RemoveSuffix(STRING src, STRING suffix) :=
            IF(EndsWith(src, suffix), src[1..length(trim(src))-length(trim(suffix))], src);


EXPORT STRING ExtractMultiple(string src, unsigned8 mask) := StringLib.StringExtractMultiple(src, mask);

EXPORT UNSIGNED4 EditDistance(string l, string r) := StringLib.EditDistance(l, r);

EXPORT BOOLEAN EditDistanceWithinRadius(string l, string r, unsigned4 radius) := StringLib.EditDistanceWithinRadius(l, r, radius);

/*
 * Returns the number of words that the string contains.  Words are separated by one or more separator strings. No
 * spaces are stripped from either string before matching.
 *
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 */

export UNSIGNED4 CountWords(STRING src, STRING separator) := BEGINC++
    if (lenSrc == 0)
        return 0;

    if ((lenSeparator == 0) || (lenSrc &lt; lenSeparator))
        return 1;

    unsigned numWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur &lt; max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                numWords++;
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
        numWords++;
    return numWords;
ENDC++;


SHARED UNSIGNED4 calcWordSetSize(STRING src, STRING separator) := BEGINC++
    if (lenSrc == 0)
        return 0;

    if ((lenSeparator == 0) || (lenSrc &lt; lenSeparator))
        return sizeof(size32_t) + lenSrc;

    unsigned sizeWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur &lt; max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                sizeWords += sizeof(size32_t) + (cur - startWord);
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
    {
        if (!startWord)
            startWord = cur;
        sizeWords += sizeof(size32_t) + (end - startWord);
    }
    return sizeWords;
ENDC++;


//Should be moved into the stringlib helper dll + single character case optimized.
SHARED SET OF STRING doSplitWords(STRING src, STRING separator, unsigned calculatedSize) := BEGINC++
    char * result = static_cast&lt;char *&gt;(rtlMalloc(calculatedsize));
    __isAllResult = false;
    __lenResult = calculatedsize;
    __result = result;

    if (lenSrc == 0)
        return;

    if ((lenSeparator == 0) || (lenSrc &lt; lenSeparator))
    {
        rtlWriteSize32t(result, lenSrc);
        memcpy(result+sizeof(size32_t), src, lenSrc);
        return;
    }

    unsigned sizeWords=0;
    const char * end = src + lenSrc;
    const char * max = end - (lenSeparator - 1);
    const char * cur = src;
    const char * startWord = NULL;
    //MORE: optimize lenSeparator == 1!
    while (cur &lt; max)
    {
        if (memcmp(cur, separator, lenSeparator) == 0)
        {
            if (startWord)
            {
                size32_t len = (cur - startWord);
                rtlWriteSize32t(result, len);
                memcpy(result+sizeof(size32_t), startWord, len);
                result += sizeof(size32_t) + len;
                startWord = NULL;
            }
            cur += lenSeparator;
        }
        else
        {
            if (!startWord)
                startWord = cur;
            cur++;
        }
    }
    if (startWord || (cur != end))
    {
        if (!startWord)
            startWord = cur;
        size32_t len = (end - startWord);
        rtlWriteSize32t(result, len);
        memcpy(result+sizeof(size32_t), startWord, len);
    }
ENDC++;

/*
 * Returns the list of words extracted from the string.  Words are separated by one or more separator strings. No
 * spaces are stripped from either string before matching.
 *
 * @param src           The string being searched in.
 * @param separator     The string used to separate words
 */

EXPORT SET OF STRING SplitWords(STRING src, STRING separator) := doSplitWords(src, separator, calcWordSetSize(src, separator));

END;&#13;&#10;
  </Attribute>
 </Module>
 <Module flags="13"
         fullname="C:\PROGRAM FILES\LEXISNEXIS\HPCC\BIN\\.\plugins\stringlib.dll"
         key="lib_stringlib"
         name="lib_stringlib"
         plugin="stringlib.dll"
         sourcePath="lib_stringlib"
         version="STRINGLIB 1.1.09">
  <Text>export StringLib := SERVICE
  string StringFilterOut(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilterOut&apos;;
  string StringFilter(const string src, const string _within) : c, pure,entrypoint=&apos;slStringFilter&apos;;
  string StringSubstituteOut(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubsOut&apos;;
  string StringSubstitute(const string src, const string _within, const string _newchar) : c, pure,entrypoint=&apos;slStringSubs&apos;;
  string StringRepad(const string src, unsigned4 size) : c, pure,entrypoint=&apos;slStringRepad&apos;;
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
END;</Text>
 </Module>
 <Module key="std.system" name="std.system">
  <Attribute key="job" name="job" sourcePath="std\system\Job.ecl">
   /*##############################################################################

    Copyright (C) &lt;2010&gt;  &lt;LexisNexis Risk Data Management Inc.&gt;

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see &lt;http://www.gnu.org/licenses/&gt;.
############################################################################## */

RETURN MODULE

/*
 * Internal functions for accessing system information relating to the current job.
 *
 * This module is provisional and subject to change without notice.
 */

shared externals :=
    SERVICE
varstring daliServer() : once, ctxmethod, entrypoint=&apos;getDaliServers&apos;;
varstring jobname() : once, ctxmethod, entrypoint=&apos;getJobName&apos;;
varstring jobowner() : once, ctxmethod, entrypoint=&apos;getJobOwner&apos;;
varstring cluster() : once, ctxmethod, entrypoint=&apos;getClusterName&apos;;
varstring platform() : once, ctxmethod, entrypoint=&apos;getPlatform&apos;;
varstring os() : once, ctxmethod, entrypoint=&apos;getOS&apos;;
unsigned integer4 logString(const varstring text) : ctxmethod, entrypoint=&apos;logString&apos;;
    END;

/*
 * How many nodes in the cluster that this code will be executed on.
 */

export nodes() := CLUSTERSIZE;

/*
 * Returns the name of the current workunit.
 */

export wuid() := WORKUNIT;

/*
 * Returns the dali server this thor is connected to.
 */

export daliServer() := externals.daliServer();

/*
 * Returns the name of the current job.
 */

export name() := externals.jobname();

/*
 * Returns the name of the user associated with the current job.
 */

export user() := externals.jobowner();

/*
 * Returns the name of the cluster the current job is targetted at.
 */

export target() := externals.cluster();

/*
 * Returns the platform type the job is running on.
 */

export platform() := externals.platform();

/*
 * Returns a string representing the target operating system.
 */

export os() := externals.os();

/*
 * Adds the string argument to the current logging context.
 *
 * @param text          The string to add to the logging.
 */

export logString(const varstring text) := externals.logString(text);

END;&#13;&#10;
  </Attribute>
 </Module>
 <Query originalFilename="C:\Users\lorraine\AppData\Local\Temp\TFRCA3E.tmp">
    #option ('targetClusterType', 'thor');
  IMPORT IMDB;
IMDB.KevinBaconNumberSets.doCounts;&#32;
 </Query>
</Archive>
