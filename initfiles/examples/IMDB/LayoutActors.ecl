/* ******************************************************************************
## Copyright © 2011 HPCC Systems.  All rights reserved.
******************************************************************************* */

// This specifies the record layout of the IMDB actors file
// The raw input file is transformed into this record structure by IMDB.FileActors
// The reason for separating this definition out is so that it can be reused in ECL code
// to write queries or analytics or any other processing

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