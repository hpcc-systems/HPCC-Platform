//This is a module which is imported and used from other queries - don't execute it.
//skip type==setup TBD

Files := $.Files(true, false);
    
EXPORT serialTest := MODULE

    EXPORT wordRec := { string word; };
    
    //A DATASET nested two levels deep
    EXPORT bookDsRec := RECORD
      string title;
      DATASET(wordRec) words;
    END;
    
    EXPORT libraryDsRec := RECORD
      string owner;
      DATASET(bookDsRec) books;
    END;
    
    
    // Same for a DICTIONARY
    EXPORT bookDictRec := RECORD
      string title
      =>
      DICTIONARY(wordRec) words;
    END;
    
    EXPORT libraryDictRec := RECORD
      string owner
      =>
      DICTIONARY(bookDictRec) books;
    END;
    
    EXPORT libraryDictionaryFile := DATASET(Files.DG_DictFilename, LibraryDictRec, THOR);
    
    EXPORT libraryDatasetFile := DATASET(Files.DG_DsFilename, LibraryDsRec, THOR);
    
    EXPORT bookIndex := INDEX({ string20 title }, { dataset(wordRec) words }, Files.DG_BookKeyFilename);
    
    EXPORT DsFilename := Files.DG_DsFilename;
    
    EXPORT DictFilename := Files.DG_DictFilename;
    
    EXPORT BookKeyFilename := Files.DG_BookKeyFilename;
    
END; /* serial */

