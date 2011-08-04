IMPORT STD.str;  //Import string library used to clean data here

//NOTE: in the original XML download, on line 35395 there is corrupt text
//that will halt this process. Before proceeding, load the XML file into
//your favorite editor and remove the following line:
//<logo src="http://musicmoz.org/img/editors/portlandpiper/aly%20bain%20&%20phil%20cunningham.jpg" />

//XML Definitions
TrackRecord := RECORD
  STRING disc{xpath('disc/@number')};
  STRING number{xpath('@number')};
    STRING tracktitle{xpath('@title')};
END;

ArtistRecord := RECORD
  STRING name{xpath('@name')};
    STRING item{xpath('item/@id')};
    STRING rtype{xpath('item/@type')};
    STRING title{xpath('item/title')};
    STRING genre{xpath('item/genre')};
    STRING releaseDate{xpath('item/releaseDate')};
    STRING formats{xpath('item/formats')};
    STRING label{xpath('item/label')};
    STRING CatalogNumber{xpath('item/cataloguenumber')};
    STRING producers{xpath('item/producers')};
    STRING guestmusicians{xpath('item/guestmusicians')};
    STRING description{xpath('item/description')};
    DATASET(TrackRecord) Tracks{XPATH('item/tracklisting/track'),MAXCOUNT(653)};
    //There is an Elvis Box Set with 30 discs and 653 tracks!
    STRING coversrc{xpath('item/cover/@src')};
END;

//DATASET allows us to clean and process XML further
// d := DATASET('~music::in::bandsartists', ArtistRecord, XML('musicmoz/category'));
d := DATASET('~file::192.168.16.111::c$::thor_trunk::ecl::musicmoz.bandsandartists.xml', ArtistRecord, XML('musicmoz/category'));
//~music::in::bandsartists
//~music::bmf::in
//************************
//New Artist record - clean and extract artist name and assign unique ID
NArtistRecord := RECORD
  UNSIGNED4  RecID;
  STRING100  artistname;
    STRING15   item;
    STRING10   rtype;
    STRING200  title;
    STRING50   genre;
    STRING10   releaseDate;
    STRING10   formats;
    STRING30   label;
    STRING30   CatalogNumber;
    STRING400  producers;
    STRING500  guestmusicians;
    STRING9000 description;
    DATASET(TrackRecord) NewTracks{MAXCOUNT(653)};
    //There is an Elvis Box Set with 30 discs and 653 tracks!
    STRING200  coversrc;
END;

NArtistRecord CleanRecs(ArtistRecord L,
                        INTEGER C) := TRANSFORM
SELF.Recid     := C;
TempString     := L.Name[21..];
InstanceSlash  := Str.Find(TempString,'/');
EndPos         := InstanceSlash - 1;
SELF.ArtistName:= TempString[1 .. EndPos];
SELF.NewTracks := L.Tracks;
SELF           := L;
END;


Clean_BandsArtists := PROJECT(d(formats<>'',
                                item<>''),
                                                                CleanRecs(LEFT,COUNTER));


//Other options applied while testing:
//Clean_BandsArtists(artistname = '311'); //Implicit output to ECL IDE
//OUTPUT(Clean_BandsArtists,{recid,artistname,item,genre});

OUTPUT(Clean_BandsArtists,,'~music::in::cleanartists');
