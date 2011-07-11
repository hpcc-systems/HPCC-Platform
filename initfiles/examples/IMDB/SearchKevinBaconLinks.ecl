/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */

// Purpose: To find the number of hops between Kevin Bacon and a given actor
// This is accomplished by 'walking' back using the two indexes in
// keys_KB.actorLinks and keys_KB.movieLinks
// So given an actor for whom we want to find links say, Jon Lovitz,
// we first find the level by doing a search using actorLinks, 
// then we find the next actor by doing a search using the movie he was
// in using the movieLinks and so on until we hit Kbacon or level 0;
//
// Usage: IMDB.SearchKevinBaconLinks('Lovitz, Jon');
    

IMPORT $ AS IMDB;
IMPORT Std;

link_rec := RECORD
  INTEGER level;
  STRING50 actor;
  STRING150 workedInMovie;
END;

link_rec show_actor(IMDB.KeysKevinBacon.ActorLinks L) := TRANSFORM
  SELF.workedInMovie := L.movie; // the movie the actor worked in
  SELF := L;
END;

link_rec show_movie(IMDB.KeysKevinBacon.MovieLinks L) := TRANSFORM
  SELF.workedInMovie := L.movie;  // the movie the actor worked in
  SELF := L;
END;

output_links (STRING find_actor) := FUNCTION
  // find the movie for the specified actor using ActorLink index
  ds := PROJECT(IMDB.KeysKevinBacon.ActorLinks(actor=find_actor), show_actor(LEFT)) ;
  // return the next level actor using MovieLinks index passing it the
  // movie retrieved via ActorLink index
  RETURN SORT(ds + PROJECT(IMDB.KeysKevinBacon.MovieLinks(movie = ds[1].workedInMovie), show_movie(LEFT)),-level) ;
END;

search_KB_links(STRING find_actor) := FUNCTION
  // what level is the actor in
  level := IMDB.KeysKevinBacon.ActorLinks(actor = find_actor)[1].level;
  ds1 := output_links(find_actor);    // the first actorLink
  ds2 := output_links(ds1[2].actor);  // the next level actor Links 
  ds3 := output_links(ds2[2].actor);  // the next level actor links .. 
  ds4 := output_links(ds3[2].actor);  // These will be null if the level is 
  ds5 := output_links(ds4[2].actor);  //  less than this level
  ds6 := output_links(ds5[2].actor);
  ds7 := output_links(ds6[2].actor);

  msg := output('Actor is at level '+Level); 
  result := CASE(level, 
    '1'=>ds1,
    '2'=>ds1&ds2,
    '3'=>ds1&ds2&ds3,
    '4'=>ds1&ds2&ds3&ds4,
    '5'=>ds1&ds2&ds3&ds4&ds5,
    '6'=>ds1&ds2&ds3&ds4&ds5&ds6,
    '7'=>ds1&ds2&ds3&ds4&ds5&ds6&ds7
    );
  return when(result, msg);
END;

string actor_name := '' : STORED('actor_name');

EXPORT SearchKevinBaconLinks(STRING find_actor = actor_name) := output(search_KB_links(find_actor));
