/* ******************************************************************************
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */

// Produce a series of sets for Actors and Movies that are : distance-0
// away (KBacons Direct movies ), distance-2 Away KBacon's Costars Movies , 
// distance-3 away - Movies of Costars of Costars etc all the way upto level 7
// 
// The nested definitions below are shown here together for the benefit of the reader. 
// 
// Notes on variable naming convention used for costars and movies
// KBMovies               :  Movies  Kevin Bacon Worked in    (distance 0)
// KBCoStars              :  Stars who worked in KBMovies      (distance 1)
// KBCoStarMovies         :  Movies worked in by KBCoStars   
//                           except KBMovies   (distance 1)
// KBCo2Stars             :  Stars(Actors) who worked in KBCoStarMovies (distance 2)
// KBCo2StarMovies        :  Movies worked  in by KBCo2Stars 
//                           except KBCoStarMovies    (distance 2)
// KBCo3Stars             :  Stars(Actors) who worked in KBCo2StarMovies (distance 3)
// KBCo3StarMovies        :  Movies worked  in by KBCo3Stars  
//                           except KBCo2StarMovies   (distance 3)
// etc..

IMPORT Std;
IMPORT $ AS IMDB;

EXPORT KevinBaconNumberSets := MODULE
  // Constructing a proper name match function is an art within itself
  // For simplicity we will define a name as matching if both first and last name are found within the string

  NameMatch(string full_name, string fname,string lname) :=
    Std.Str.Find(full_name,fname,1) > 0 AND 
    Std.Str.Find(full_name,lname,1) > 0;

  //------ Get KBacon Movies
  AllKBEntries := IMDB.ActorsInMovies(NameMatch(actor,'Kevin','Bacon'));
  EXPORT KBMovies := DEDUP(AllKBEntries, movie, ALL); // Each movie should ONLY occur once

  //------ Get KBacon CoStars
  CoStars := IMDB.ActorsInMovies(Movie IN SET(KBMovies,Movie));
  EXPORT KBCoStars := DEDUP( CoStars(actor<>'Kevin Bacon'), actor, ALL);

  //------ Get KBacon Costars' Movies
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

  // KCCo2S = ALL Actors appearing in Movies of KBacon's CoActors
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
  // However it is worth looking at the counts first; we may be down to a small enough set that we can start
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
    OUTPUT(COUNT(KBMovies), NAMED('KBMovies')),
    OUTPUT(COUNT(KBCoStars), NAMED('KBCoStars')),
    OUTPUT(COUNT(KBCoStarMovies), NAMED('KBCoStarMovies')),
    OUTPUT(COUNT(KBCo2Stars), NAMED('KBCo2Stars')),
    OUTPUT(COUNT(KBCo2StarMovies), NAMED('KBCo2StarMovies')),
    OUTPUT(COUNT(KBCo3Stars), NAMED('KBCo3Stars')),
    OUTPUT(COUNT(KBCo3StarMovies), NAMED('KBCo3StarMovies')),
    OUTPUT(COUNT(KBCo4Stars), NAMED('KBCo4Stars')),
    OUTPUT(COUNT(KBCo4StarMovies), NAMED('KBCo4StarMovies')),
    OUTPUT(COUNT(KBCo5Stars), NAMED('KBCo5Stars')),
    OUTPUT(COUNT(KBCo5StarMovies), NAMED('KBCo5StarMovies')),
    OUTPUT(COUNT(KBCo6Stars), NAMED('KBCo6Stars')),
    OUTPUT(COUNT(KBCo6StarMovies), NAMED('KBCo6StarMovies')),
    OUTPUT(COUNT(KBCo7Stars), NAMED('KBCo7Stars')),
    OUTPUT(KBCo7Stars)
  );

END;

