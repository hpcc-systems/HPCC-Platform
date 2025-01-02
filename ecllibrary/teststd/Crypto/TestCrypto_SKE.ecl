/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;


EXPORT TestCrypto_SKE := MODULE

  EXPORT TestSupportedSKE := MODULE
    EXPORT TS01 := ASSERT(Std.Crypto.SupportedSymmetricCipherAlgorithms() = ['aes-256-cbc', 'aes-192-cbc', 'aes-128-cbc']);
  END;

  EXPORT TestSKE01 := MODULE
    EXPORT mod := Std.Crypto.SymmetricEncryption( 'aes-256-cbc', '01234567890123456789012345678901' );
    EXPORT DATA dat := mod.Encrypt(           (DATA)'256The quick brown fox jumps over the lazy dog');
    EXPORT TS01  := ASSERT(mod.Decrypt(dat) = (DATA)'256The quick brown fox jumps over the lazy dog');
    EXPORT TS011 := ASSERT(mod.Decrypt(dat) != (DATA)'Hello World');
  END;

  EXPORT TestSKE02 := MODULE
    EXPORT mod := Std.Crypto.SymmetricEncryption( 'aes-192-cbc', '012345678901234567890123' );
    EXPORT DATA dat := mod.Encrypt(          (DATA)'192The quick brown fox jumps over the lazy dog');
    EXPORT TS02 := ASSERT(mod.Decrypt(dat) = (DATA)'192The quick brown fox jumps over the lazy dog');
  END;

  EXPORT TestSKE03 := MODULE
    EXPORT mod := Std.Crypto.SymmetricEncryption( 'aes-128-cbc', '0123456789012345' );
    EXPORT DATA dat := mod.Encrypt(          (DATA)'128The quick brown fox jumps over the lazy dog');
    EXPORT TS03 := ASSERT(mod.Decrypt(dat) = (DATA)'128The quick brown fox jumps over the lazy dog');
  END;

  EXPORT TestSKE04 := MODULE
    EXPORT mod := Std.Crypto.SymmetricEncryption( 'aes-256-cbc', '01234567890123456789012345678901' );
    EXPORT DATA dat := mod.Encrypt(          (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');
    EXPORT TS04 := ASSERT(mod.Decrypt(dat) = (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ');

    EXPORT DATA dat2 := mod.Encrypt(          (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<');
    EXPORT TS05 := ASSERT(mod.Decrypt(dat2) = (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<');
  END;

  EXPORT TestSKE05 := MODULE // HPCC-33157
    EXPORT mod := Std.Crypto.SymmetricEncryption('aes-256-cbc', '01234567890123456789012345678901');
    EXPORT TS05 := ASSERT(mod.Decrypt((DATA)'') = (DATA)'');
  END;
END;

