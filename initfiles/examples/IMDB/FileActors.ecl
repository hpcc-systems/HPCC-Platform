/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */
    
// Process and transform the raw sprayed files for actor and actresses into formats 
// more suitable for subsequent processing & analysis       
//
// This strips out the headers and footers from the files, combine them into
// one file, eliminate some 'dirty' or incomplete records, and parse the free form
// text lines into formatted records.

IMPORT $ AS IMDB;
IMPORT Std;

base:= IF(Std.System.Job.platform()='standalone', '', '~thor::in::IMDB::') : GLOBAL;

raw_rec:= RECORD
    STRING rawtext {maxlength(1024)};
    STRING6 gender;
END;

// HEADING is to strip the pre-amble ...
// A different solution would be to look for some kind of data element in the pre-able at filter everything before that ...
// This would be safer if the length of the pre-amble changes
// Of course if the last line of the pre-amble changed we would still be dead!
// QUOTE is required as some of the 'quotes' in the text are unbalanced
// SEPARATOR is used to allow the tabs through unscathed
ds_raw_male  := DATASET(base+'actors.list',raw_rec,CSV(HEADING(239),SEPARATOR('|'),QUOTE([]) ));

ds_raw_female:= DATASET(base+'actresses.list',raw_rec,CSV(HEADING(241),SEPARATOR('|'),QUOTE([])));

// Here we are going to remove multiple tab sequences to produce one single tab separator
// makes downstream process a lot cleaner
// We are also going to join actors and actresses together but retain gender information in a separate field.
raw_rec StripDoubleTab(raw_rec le, string6 G) := TRANSFORM
  SELF.rawtext := Std.Str.FindReplace(Std.Str.FindReplace(le.rawtext,'\t\t\t','\t'),'\t\t','\t');
  SELF.Gender  := G;
END;

raw_male := PROJECT(ds_raw_male(rawtext<>''),StripDoubleTab(LEFT,'MALE'));
raw_female := PROJECT(ds_raw_female(rawtext<>''),StripDoubleTab(LEFT,'FEMALE'));


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
    UNSIGNED2 TabPos := Std.Str.Find(L.RawText,'\t',1); // Variables are local to transform to save typing
    BOOLEAN HasName  := TabPos > 0;
    SELF.actorname   := IF ( HasName, IMDB.CleanActor(L.RawText[1..TabPos-1]), '' );
    SELF.filmname    := if ( HasName, L.RawText[TabPos+1..],L.RawText );
    SELF             := L; // Only moving Gender presently;
  END;

  //PROJECT the input Dataset using the prepActorFilmNames transform
  prep_ds1           := PROJECT(ds_in, prepActorFilmNames(LEFT));

  //For those records that do not have an actorname it is possible to move it from the previous record
  // In the transform for an iterate the R is the 'current' record
  // L is the 'last' one so that we can copy any data we wish
  layout_actor_rec FillActor(prep_ds1 L, prep_ds1 R) := TRANSFORM
    SELF.actorname := IF( r.actorname = '',L.ActorName,R.ActorName );
    SELF           := R;
  END;

  // Iterate the prep dataset using the FillActor Transform to get the structure we want 
  RETURN ITERATE(prep_ds1,FillActor(LEFT,RIGHT));
END;

// The iterate is now done so male and female can be safely processed as one database
ThreeColumns := into_three(raw_male)+into_three(raw_female);

// Now we have three fields defined with want to dig into the filmname format to parse out some of the extra information into fields
// We have placed our target layout in a separate ECL source file; it could be inline but it is nice
// to be able to see the data format without having to wade through all the low level code.

// Here we encapsulate the code to pull data out from between boundary markers such as (){} etc
FindWithin(string Src,String ToFind,String ToEnd) := 
   IF ( Std.Str.Find(Src,ToFind,1) > 0,
        Src[Std.Str.Find(Src,ToFind,1)+length(ToFind)..Std.Str.Find(Src,ToEnd,1)-1],
        '' );

// This transform plucks the data field by field out of the 'filmname' which has 'magic symbols' in the 3 column format
IMDB.LayoutActors tActors(ThreeColumns L) := TRANSFORM

  // TV series are enclosed within ""
  self.isTVSeries   := IF( L.filmname[1] = '"', 'Y','N' );

  // The movie name is the first thing on the line and will be followed by the year in ()
  // If it is a TV program it will be in "
  self.moviename    := IF (L.filmname[1]='"', L.filmname[2..Std.Str.Find(L.filmname,'"',2)-1],L.filmname[1..Std.Str.Find(L.filmname,'(',1)-1]);
  self.movie_type   := MAP ( Std.Str.Find(L.filmname,'(TV)',1) > 0 => 'For TV',
                             Std.Str.Find(L.filmname,'(V)',1) > 0 => 'Video',
                             '');
  self.year         := FindWithin(L.filmname,'(',')');
  self.rolename     := FindWithin(L.filmname,'[',']');
  self.credit_pos   := (integer2)FindWithin(L.filmname,'<','>');
  self.episode_name := FindWithin(L.filmname,'{','}');
  self.episode_num  := FindWithin(L.filmname,'(#',')');
  SELF              := L; // Copy across everything but movie name
END;

// The year filter is a quick fix of tackling the actual problem  which is 
// that we have a 'tail' on the file which contains 'junk' as far as the
// database is concerned..This solution filters out all date-less entries; 
// some of these are NOT in the tail - but all of them are non valid anyway.

EXPORT FileActors := PROJECT(ThreeColumns,tActors(LEFT))(year<>'');
