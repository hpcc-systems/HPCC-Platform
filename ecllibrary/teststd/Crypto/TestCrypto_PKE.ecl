/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;


EXPORT TestCrypto_PKE := MODULE


EXPORT STRING pubKey := '-----BEGIN PUBLIC KEY-----' + '\n' +
'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAr64RncTp5pV0KMnWRAof' + '\n' +
'od+3AUS/IDngT39j3Iovv9aI2N8g4W5ipqhKftRESmzQ6I/TiUQcmi42soUXmCeE' + '\n' +
'BHqlMDydw9aHOQG17CB30GYsw3Lf8iZo7RC7ocQE3OcRzH0eBkOryW6X3efWnMoy' + '\n' +
'hIR9MexCldF+3WM/X0IX0ApSs7kuVPVG4Yj202+1FVO/XNwjMukJG5ASuxpYAQvv' + '\n' +
'/oKj6q7kInEIvhLiGfcm3bpTzWQ66zVz3z/huLbEXEy5oj2fQaC5E3s5mdpk/CW3' + '\n' +
'J6Tk4NY3NySWzE/2/ZOWxZdR79XC+goNL6v/5gPI8B/a3Z8OeM2PfSZwPMnVuvU0' + '\n' +
'bwIDAQAB' + '\n' +
'-----END PUBLIC KEY-----';


EXPORT STRING privKey := '-----BEGIN RSA PRIVATE KEY-----' + '\n' +
'MIIEowIBAAKCAQEAr64RncTp5pV0KMnWRAofod+3AUS/IDngT39j3Iovv9aI2N8g' + '\n' +
'4W5ipqhKftRESmzQ6I/TiUQcmi42soUXmCeEBHqlMDydw9aHOQG17CB30GYsw3Lf' + '\n' +
'8iZo7RC7ocQE3OcRzH0eBkOryW6X3efWnMoyhIR9MexCldF+3WM/X0IX0ApSs7ku' + '\n' +
'VPVG4Yj202+1FVO/XNwjMukJG5ASuxpYAQvv/oKj6q7kInEIvhLiGfcm3bpTzWQ6' + '\n' +
'6zVz3z/huLbEXEy5oj2fQaC5E3s5mdpk/CW3J6Tk4NY3NySWzE/2/ZOWxZdR79XC' + '\n' +
'+goNL6v/5gPI8B/a3Z8OeM2PfSZwPMnVuvU0bwIDAQABAoIBAQCnGAtNYkOOu8wW' + '\n' +
'F5Oid3aKwnwPytF211WQh3v2AcFU17qle+SMRi+ykBL6+u5RU5qH+HSc9Jm31AjW' + '\n' +
'V1yPrdYVZInFjYIJCPzorcXY5zDOmMAuzg5PBVV7VhUA0a5GZck6FC8AilDUcEom' + '\n' +
'GCK6Ul8mR9XELBFQ6keeTo2yDu0TQ4oBXrPBMN61uMHCxh2tDb2yvl8Zz+EllADG' + '\n' +
'70pztRWNOrCzrC+ARlmmDfYOUgVFtZin53jq6O6ullPLzhkm3/+QFRGYWsFgQB6J' + '\n' +
'Z9HJtW5YB47RT5RbLHKXeMc6IJW+d+5HrzgTdK79P7wAZk8JCIDyHe2AaNAUzc/G' + '\n' +
'sB0cNeURAoGBAOKtaVFa6z2F4Q+koMBXCt4m7dCJnaC+qthF249uEOIBeF3ds9Fq' + '\n' +
'f0jhhvuV0OcN8lYbR/ZlYRJDUs6mHh/2BYSkdeaLKojXTxKR2bA4xQk5dtJCdoPf' + '\n' +
'0c15AlTgOYk2oNXP/azDICJYT/cdvIdUL9P4IoZthu1FjwG266GacEnNAoGBAMZn' + '\n' +
'1wRUXS1dbqemoc+g48wj5r3/qsIG8PsZ2Y8W+oYW7diNA5o6acc8YPEWE2RbJDbX' + '\n' +
'YEADBnRSdzzOdo0JEj4VbNZEtx6nQhBOOrtYKnnqHVI/XOz3VVu6kedUKdBR87KC' + '\n' +
'eCzO1VcEeZtsTHuLO4t7NmdHGqNxTV+jLvzBoQsrAoGAI+fOD+nz6znirYSpRe5D' + '\n' +
'tW67KtYxlr28+CcQoUaQ/Au5kjzE9/4DjXrT09QmVAMciNEnc/sZBjiNzFf525wv' + '\n' +
'wZP/bPZMVYKtbsaVkdlcNJranHGUrkzswbxSRzmBQ5/YmCWrDAuYcnhEqmMWcuU9' + '\n' +
'8jiS13JP9hOXlHDyIBYDhV0CgYBV6TznuQgnzp9NpQ/H8ijxilItz3lHTu4mLMlR' + '\n' +
'9mdAjMkszdLTg5uuE+z+N8rp17VUseoRjb3LvLG4+MXIyDbH/0sDdPm+IjqvCNDR' + '\n' +
'spmh9MgBh0JbsbWaZK0s9/qrI/FcSLZ04JLsfRmTPU/Y5y8/dHjYO6fDQhp44RZF' + '\n' +
'iCqNxQKBgHf7KZIOKgV4YNyphk1UYWHNz8YY5o7WtaQ51Q+kIbU8PRd9rqJLZyk2' + '\n' +
'tKf8e6z+wtKjxi8GKQzE/IdkQqiFmB1yEjjRHQ81WS+K5NnjN1t0IEscJqOAwv9s' + '\n' +
'iIhG5ueb6xoj/N0LuXa8loUT5aChKWxRHEYdegqU48f+qxUcJj9R' + '\n' +
'-----END RSA PRIVATE KEY-----';

  EXPORT TestSupportedPKE := MODULE
    EXPORT TS01 := ASSERT(Std.Crypto.SupportedPublicKeyAlgorithms() = ['RSA']) : ONWARNING(2364, IGNORE);
  END;

  //Encrypt/Decrypt

  EXPORT TestPKE01 := MODULE
    EXPORT mod := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKey, privKey, '') : ONWARNING(2364, IGNORE);
    EXPORT DATA dat1 := mod.Encrypt(            (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
    EXPORT TS01  := ASSERT(mod.Decrypt(dat1)  = (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
  
    EXPORT TS011 := ASSERT(mod.Decrypt(dat1) != (DATA)'Hello World') : ONWARNING(2364, IGNORE);

    EXPORT DATA dat2 := mod.Encrypt(           (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);
    EXPORT TS012 := ASSERT(mod.Decrypt(dat2) = (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);

    EXPORT DATA dat3 := mod.Encrypt(           (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
    EXPORT TS013 := ASSERT(mod.Decrypt(dat3) = (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
  END;

  EXPORT TestPKE02 := MODULE
    EXPORT mod := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKey, privKey, '0123456789') : ONWARNING(2364, IGNORE);
    EXPORT DATA dat1 := mod.Encrypt(            (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
    EXPORT TS02  := ASSERT(mod.Decrypt(dat1)  = (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
  
    EXPORT TS021 := ASSERT(mod.Decrypt(dat1) != (DATA)'Hello World') : ONWARNING(2364, IGNORE);

    EXPORT DATA dat2 := mod.Encrypt(           (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);
    EXPORT TS022 := ASSERT(mod.Decrypt(dat2) = (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);

    EXPORT DATA dat3 := mod.Encrypt(           (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
    EXPORT TS023 := ASSERT(mod.Decrypt(dat3) = (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
  END;

  //Digital Signatures

  EXPORT TestPKE03 := MODULE
    EXPORT mod := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKey, privKey, '') : ONWARNING(2364, IGNORE);
    EXPORT DATA sig1 := mod.Sign(                           (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
    EXPORT TS03 := ASSERT( TRUE = mod.VerifySignature(sig1, (DATA)'The quick brown fox jumps over the lazy dog')) : ONWARNING(2364, IGNORE);
 
    EXPORT TS031:= ASSERT( FALSE = mod.VerifySignature(sig1, (DATA)'Hello World')) : ONWARNING(2364, IGNORE);

    EXPORT DATA sig2 := mod.Sign(                            (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);
    EXPORT TS032 := ASSERT( TRUE = mod.VerifySignature(sig2, (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')) : ONWARNING(2364, IGNORE);

    EXPORT DATA sig3 := mod.Sign(                           (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
    EXPORT S033 := ASSERT( TRUE = mod.VerifySignature(sig3, (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<')) : ONWARNING(2364, IGNORE);
  END; 

  EXPORT TestPKE04 := MODULE
    EXPORT mod := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKey, privKey, '0123456789') : ONWARNING(2364, IGNORE);
    EXPORT DATA sig1 := mod.Sign(                           (DATA)'The quick brown fox jumps over the lazy dog') : ONWARNING(2364, IGNORE);
    EXPORT TS03 := ASSERT( TRUE = mod.VerifySignature(sig1, (DATA)'The quick brown fox jumps over the lazy dog')) : ONWARNING(2364, IGNORE);
 
    EXPORT TS031:= ASSERT( FALSE = mod.VerifySignature(sig1, (DATA)'Hello World')) : ONWARNING(2364, IGNORE);

    EXPORT DATA sig2 := mod.Sign(                            (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') : ONWARNING(2364, IGNORE);
    EXPORT TS032 := ASSERT( TRUE = mod.VerifySignature(sig2, (DATA)'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')) : ONWARNING(2364, IGNORE);

    EXPORT DATA sig3 := mod.Sign(                           (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<') : ONWARNING(2364, IGNORE);
    EXPORT S033 := ASSERT( TRUE = mod.VerifySignature(sig3, (DATA)'0123456789~`!@#$%^&*()-_=+|][}{;:?.>,<')) : ONWARNING(2364, IGNORE);
  END; 
END;

