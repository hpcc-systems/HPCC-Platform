/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.  All rights reserved.
############################################################################## */


EXPORT Type := MODULE

  /**
   * Type.vrec(x) deals with translations to arbitrary record type x
   * MORE - think of a better name than vrec!
   */
     
  EXPORT VRec(virtual record outrec) := MODULE

    SHARED externals := SERVICE : fold
      STRING dumpRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='dumpRecordType';
      DATA serializeRecordType(virtual record val) : eclrtl,pure,library='eclrtl',entrypoint='serializeRecordType';

      streamed dataset(outrec) translate(streamed dataset input) : eclrtl,pure,library='eclrtl',entrypoint='transformDataset',passParameterMeta(true);
      _linkcounted_ row(outrec) translateRow(virtual record outrec, ROW indata) : eclrtl,pure,library='eclrtl',entrypoint='transformRecord',passParameterMeta(true);

      DATA serializeRow(virtual record outrec, ROW inRow) : eclrtl,pure,library='eclrtl',entrypoint='serializeRow',passParameterMeta(true),fold;
      _linkcounted_ row(outrec) deserializeRow(virtual record outrec, DATA parcel) : eclrtl,pure,library='eclrtl',entrypoint='deserializeRow',passParameterMeta(true),fold;
      
      SET OF STRING getFieldNames(virtual record meta) : eclrtl,pure,library='eclrtl',entrypoint='getFieldNames',fold,passParameterMeta(true);
      UNSIGNED4 getNumFields(virtual record meta) : eclrtl,pure,library='eclrtl',entrypoint='getNumFields',fold,passParameterMeta(true);

      unsigned4 getFieldNum(virtual record meta, CONST VARSTRING name) : eclrtl,pure,library='eclrtl',entrypoint='getFieldNum',fold,passParameterMeta(true);
      string getFieldName(virtual record meta, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='getFieldName',fold,passParameterMeta(true);
      string getFieldType(virtual record meta, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='getFieldType',fold,passParameterMeta(true);
      integer readSerializedInt(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedInt',fold,passParameterMeta(true);
      unsigned readSerializedUInt(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedUInt',fold,passParameterMeta(true);
      string readSerializedString(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedString',fold,passParameterMeta(true);
      real8 readSerializedReal(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedReal',fold,passParameterMeta(true);
      boolean readSerializedBool(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedBool',fold,passParameterMeta(true);
      utf8 readSerializedUtf8(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedUtf8',fold,passParameterMeta(true);
      data readSerializedData(virtual record meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedData',fold,passParameterMeta(true);
    end;
    
    /*
     * Dynamic translation to arbitrary record structure
     */
    EXPORT outrec translateRow(ROW inrow) := externals.translateRow(outrec, inrow);
    EXPORT streamed dataset(outrec) translate(streamed dataset indata) := externals.translate(indata);
    
    /**
     * Returns the binary type metadata structure for a record.
     * 
     * @return           A binary representation  of the type information
     */
    EXPORT DATA getBinaryTypeInfo() := externals.serializeRecordType(outrec);
    /**
     * Returns the json type metadata structure for a record.
     * 
     * @return           A json representation  of the type information
     */
    EXPORT STRING getJsonTypeInfo() := externals.dumpRecordType(outrec);
     
    EXPORT SET OF STRING fieldNames() := externals.getFieldNames(outrec);
    EXPORT UNSIGNED4 numFields() := externals.getNumFields(outrec);
    
    /**
     * Returns a serialized copy of a row.
     * @param  inrow  Input row, in arbitrary layout. This will be translated to output layout then serialized.
     * @return        Serialized data for the resulting output row
     */
    EXPORT DATA serialize(ROW inrow) := externals.serializeRow(outrec, inrow);

    /**
     * Deserializes serialized row to an ECL row in specified layout.
     * @param  rowdata Data to deserialize. This should have been created using serialize() with matching output format, or results will be unpredictable.
     * @return         Deserialized data for the resulting output row
     */
    EXPORT outrec deserialize(DATA rowdata) := externals.deserializeRow(outrec, rowdata);
    
    /**
     * Functions to access individual fields within a serialized row without having to deserialize the whole row
     * @param  parcel    Parcel to access. This should have been created using serialize() with matching output format, or results will be unpredictable.
     * @param  fieldname Name of field to access.
     */
    EXPORT deserializedField(DATA parcel, STRING fieldname) := MODULE
      EXPORT STRING   readString() := externals.readSerializedString(outrec, parcel, externals.getFieldNum(outrec, fieldname));;
      EXPORT INTEGER  readInt() := externals.readSerializedInt(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT REAL8    readReal() := externals.readSerializedReal(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT BOOLEAN  readBool() := externals.readSerializedBool(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT UTF8     readUTF8() := externals.readSerializedUtf8(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT DATA     readData() := externals.readSerializedData(outrec, parcel, externals.getFieldNum(outrec, fieldname));
    END;

    /**
     * Retrieve field name for a given field. Field indexes start at 1
     * @param  fieldIndex  Field number to lookup
     * @return             Corresponding field name, or blank if not found.
     */
    EXPORT STRING getFieldName(UNSIGNED fieldIndex) := externals.getFieldName(outrec, fieldIndex);
    
    /**
     * Retrieve field index for a given field. Field indexes start at 1
     * @param  fieldName   Field name to lookup
     * @return             Corresponding field index, or 0 if not found.
     */
    EXPORT UNSIGNED getFieldNum(STRING fieldName) := externals.getFieldNum(outrec, fieldName);

    /**
     * Retrieve field type for a given field.
     * @param  fieldName   Field name to lookup
     * @return             Corresponding field type, or blank if not found.
     */
    EXPORT STRING getFieldType(STRING fieldName) := externals.getFieldType(outrec, getFieldNum(fieldName));
    

  END;
  
  EXPORT Rec(ROW rec) := MODULE
    SHARED externals := SERVICE : fold
      DATA serializeRow(ROW inRow) : eclrtl,pure,library='eclrtl',entrypoint='serializeRow',passParameterMeta(true),fold;

      unsigned4 getFieldNum(virtual record meta, CONST VARSTRING name) : eclrtl,pure,library='eclrtl',entrypoint='getFieldNum',fold,passParameterMeta(true);
      string getFieldName(virtual record meta, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='getFieldName',fold,passParameterMeta(true);

      integer readFieldInt(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldInt',fold,passParameterMeta(true);
      string readFieldString(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldString',fold,passParameterMeta(true);
      real8 readFieldReal(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldReal',fold,passParameterMeta(true);
      boolean readFieldBool(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldBool',fold,passParameterMeta(true);
      utf8 readFieldUtf8(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldUtf8',fold,passParameterMeta(true);
      data readFieldData(RECORDOF(rec) row, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readFieldData',fold,passParameterMeta(true);
    END;

    /**
     * Returns a serialized copy of a row.
     * @return        Serialized data for the supplied row
     */
    EXPORT DATA serialize() := externals.serializeRow(rec);


    /**
     * Functions to access individual fields within a record
     * @param  fieldname Name of field to access.
     */
    EXPORT field(STRING fieldname) := MODULE
      EXPORT STRING   readString() := externals.readFieldString(rec, externals.getFieldNum(rec, fieldname));;
      EXPORT INTEGER  readInt() := externals.readFieldInt(rec, externals.getFieldNum(rec, fieldname));
      EXPORT REAL8    readReal() := externals.readFieldReal(rec, externals.getFieldNum(rec, fieldname));
      EXPORT BOOLEAN  readBool() := externals.readFieldBool(rec, externals.getFieldNum(rec, fieldname));
      EXPORT UTF8     readUTF8() := externals.readFieldUtf8(rec, externals.getFieldNum(rec, fieldname));
      EXPORT DATA     readData() := externals.readFieldData(rec, externals.getFieldNum(rec, fieldname));
    END;
    
    /**
     * Retrieve field name for a given field. Field indexes start at 1
     * @param  fieldIndex  Field number to lookup
     * @return             Corresponding field name, or blank if not found.
     */
    EXPORT STRING getFieldName(UNSIGNED fieldIndex) := externals.getFieldName(rec, fieldIndex);
    
    /**
     * Retrieve field index for a given field. Field indexes start at 1
     * @param  fieldName   Field name to lookup
     * @return             Corresponding field index, or 0 if not found.
     */
    EXPORT UNSIGNED getFieldNum(STRING fieldnum) := externals.getFieldNum(rec, fieldnum);

  END;

/**
 * DRec module provides a subset of the same functionality as VRec, but the type information is supplied as serialized data rather than as a virtual record
 */
  EXPORT DRec(DATA outrec) := MODULE

    SHARED externals := SERVICE : fold
      DATA serializeRow(DATA outrec, ROW inRow) : eclrtl,pure,library='eclrtl',entrypoint='serializeRow';
      SET OF STRING getFieldNames(DATA meta) : eclrtl,pure,library='eclrtl',entrypoint='getFieldNames';
      UNSIGNED4 getNumFields(DATA meta) : eclrtl,pure,library='eclrtl',entrypoint='getNumFields';
      unsigned4 getFieldNum(DATA meta, CONST VARSTRING name) : eclrtl,pure,library='eclrtl',entrypoint='getFieldNum';
      string getFieldName(DATA meta, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='getFieldName';
      string getFieldType(DATA meta, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='getFieldType';
      integer readSerializedInt(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedInt';
      unsigned readSerializedUInt(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedUInt';
      string readSerializedString(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedString';
      real8 readSerializedReal(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedReal';
      boolean readSerializedBool(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedBool';
      utf8 readSerializedUtf8(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedUtf8';
      data readSerializedData(DATA meta, DATA parcel, unsigned4 fieldNum) : eclrtl,pure,library='eclrtl',entrypoint='readSerializedData';
    end;
    
    /**
     * Returns the binary type metadata structure for a record.
     * 
     * @return           A binary representation  of the type information
     */
    EXPORT DATA getBinaryTypeInfo() := outrec;
     
    EXPORT SET OF STRING fieldNames() := externals.getFieldNames(outrec);
    EXPORT UNSIGNED4 numFields() := externals.getNumFields(outrec);
    
    /**
     * Returns a serialized copy of a row.
     * @param  inrow  Input row, in arbitrary layout. This will be translated to output layout then serialized.
     * @return        Serialized data for the resulting output row
     */
    EXPORT DATA serialize(ROW inrow) := externals.serializeRow(outrec, inrow);

    /**
     * Functions to access individual fields within a serialized row without having to deserialize the whole row
     * @param  parcel    Parcel to access. This should have been created using serialize() with matching output format, or results will be unpredictable.
     * @param  fieldname Name of field to access.
     */
    EXPORT deserializedField(DATA parcel, STRING fieldname) := MODULE
      EXPORT STRING   readString() := externals.readSerializedString(outrec, parcel, externals.getFieldNum(outrec, fieldname));;
      EXPORT INTEGER  readInt() := externals.readSerializedInt(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT REAL8    readReal() := externals.readSerializedReal(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT BOOLEAN  readBool() := externals.readSerializedBool(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT UTF8     readUTF8() := externals.readSerializedUtf8(outrec, parcel, externals.getFieldNum(outrec, fieldname));
      EXPORT DATA     readData() := externals.readSerializedData(outrec, parcel, externals.getFieldNum(outrec, fieldname));
    END;

    /**
     * Retrieve field name for a given field. Field indexes start at 1
     * @param  fieldIndex  Field number to lookup
     * @return             Corresponding field name, or blank if not found.
     */
    EXPORT STRING getFieldName(UNSIGNED fieldIndex) := externals.getFieldName(outrec, fieldIndex);
    
    /**
     * Retrieve field index for a given field. Field indexes start at 1
     * @param  fieldName   Field name to lookup
     * @return             Corresponding field index, or 0 if not found.
     */
    EXPORT UNSIGNED getFieldNum(STRING fieldName) := externals.getFieldNum(outrec, fieldName);

    /**
     * Retrieve field type for a given field.
     * @param  fieldName   Field name to lookup
     * @return             Corresponding field type, or blank if not found.
     */
    EXPORT STRING getFieldType(STRING fieldName) := externals.getFieldType(outrec, getFieldNum(fieldName));

  END;
  

END;
