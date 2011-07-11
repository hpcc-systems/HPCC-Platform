/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */

// Produce 2 index files for faster access to the KevinBaconNumberSets defined earlier.
// These index files will also allow us to 'walk' back from a given actor to 
// Kevin Bacon to list the number of hops. 
// INDEX1 : Movie_Links with a structure of {movie:actor:level}
// INDEX2 : Actor_Links with a structure of {actor:movie:level}

IMPORT $ AS IMDB;
IMPORT Std;

EXPORT KeysKevinBacon := MODULE

  // We want a key which shows the 'Bacon level' of the movie 
  // and the actor in the movie that is closest to Kevin Bacon

  movie_layout := RECORD
    STRING150 movie; // First element is the one to be searched upon
    STRING50 actor;
    UNSIGNED level;
  END;

  movie_layout into(IMDB.ActorsInMovies L, UNSIGNED level) := TRANSFORM
    SELF.level := level;
    SELF := L;
  END;

  //Movie set references
  KBMovies := IMDB.KevinBaconNumberSets.KBMovies;
  KBCoStarMovies := IMDB.KevinBaconNumberSets.KBCoStarMovies;
  KBCo2StarMovies := IMDB.KevinBaconNumberSets.KBCo2StarMovies;
  KBCo3StarMovies := IMDB.KevinBaconNumberSets.KBCo3StarMovies;
  KBCo4StarMovies := IMDB.KevinBaconNumberSets.KBCo4StarMovies;
  KBCo5StarMovies := IMDB.KevinBaconNumberSets.KBCo5StarMovies;
  KBCo6StarMovies := IMDB.KevinBaconNumberSets.KBCo6StarMovies; 

  movie_links := PROJECT(KBMovies,into(LEFT, 0))
                +PROJECT(KBCoStarMovies, into(LEFT, 1))
                +PROJECT(KBCo2StarMovies, into(LEFT, 2))
                +PROJECT(KBCo3StarMovies, into(LEFT, 3))
                +PROJECT(KBCo4StarMovies, into(LEFT, 4))
                +PROJECT(KBCo5StarMovies, into(LEFT, 5))
                +PROJECT(KBCo6StarMovies, into(LEFT, 6));

  EXPORT MovieLinks := INDEX(movie_links, {movie_links}, '~temp::movie_links_v2');
  
  // We want a key which shows the 'Bacon level' of the actor and the movie that is closest to Kevin Bacon
  
  actor_layout:= RECORD
    STRING50  actor; // First element is the one you will search on
    STRING150 movie;
    UNSIGNED  level;
  END;

  actor_layout into(IMDB.ActorsInMovies L, UNSIGNED level) := TRANSFORM
    SELF.level := level;
    SELF := L;
  END;

  //Actor set reference
  KBCoStars  := IMDB.KevinBaconNumberSets.KBCoStars;
  KBCo2Stars := IMDB.KevinBaconNumberSets.KBCo2Stars;
  KBCo3Stars := IMDB.KevinBaconNumberSets.KBCo3Stars;
  KBCo4Stars := IMDB.KevinBaconNumberSets.KBCo4Stars;
  KBCo5Stars := IMDB.KevinBaconNumberSets.KBCo5Stars;
  KBCo6Stars := IMDB.KevinBaconNumberSets.KBCo6Stars;
  KBCo7Stars := IMDB.KevinBaconNumberSets.KBCo7Stars;

  actor_links := PROJECT(KBCoStars, into(LEFT, 1))
                +PROJECT(KBCo2Stars, into(LEFT, 2))
                +PROJECT(KBCo3Stars, into(LEFT, 3))
                +PROJECT(KBCo4Stars, into(LEFT, 4))
                +PROJECT(KBCo5Stars, into(LEFT, 5))
                +PROJECT(KBCo6Stars, into(LEFT, 6))
                +PROJECT(KBCo7Stars, into(LEFT, 7));

  EXPORT ActorLinks := INDEX(actor_links,{actor_links},'~temp::actor_links_v2');
  
  EXPORT BuildAll := PARALLEL(
    BUILDINDEX(ActorLinks, OVERWRITE), 
    BUILDINDEX(MovieLinks, OVERWRITE)
  );

END;
