/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */

/**
  * Produce a slimmed down version of the IMDB actor AND actress files to 
  * permit more efficient join operations.
  * Filter out the movie records we do not want in building our KBacon Number sets.
  *
  */

IMPORT $ AS IMDB;
IMPORT Std;

// Filter out TV movies, Videos AND some documentary type collections
ds_IMDB := IMDB.FileActors(actorname!='' AND moviename != '' AND
                           Std.Str.Find(moviename,'Boffo',1) = 0 AND
                           Std.Str.Find(moviename,'Slasher Film',1) = 0 AND
                           movie_type != 'Video' AND isTVseries = 'N' AND
                           movie_type != 'For TV');

//Slim the records down to bare essentials for searching AND joining
slim_IMDB_rec := RECORD
  STRING50  actor;
  STRING150 movie;
END;

slim_IMDB_rec slim_it(ds_IMDB L):= TRANSFORM
  SELF.actor := Std.Str.FindReplace(L.actorname,'(I)','');
  SELF.movie := L.moviename;;
END;

IMDB_names := PROJECT(ds_IMDB, slim_it(LEFT));

export ActorsInMovies := IMDB_Names : persist('~temp::IMDB::ActorsInMovies');;
