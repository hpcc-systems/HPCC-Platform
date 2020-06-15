/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */


EXPORT Crypto := MODULE


IMPORT lib_cryptolib;


/**
 * Returns set of supported Hash Algorithms
 *
 * @return        SET OF STRING containing all supported Hash Algorithms
 */
EXPORT SET OF STRING SupportedHashAlgorithms() := lib_cryptolib.CryptoLib.SupportedHashAlgorithms();


/**
 * Returns set of supported CipherAlgorithms
 *
 * @return        SET OF STRING containing all supported Cipher Algorithms
 */
EXPORT SET OF STRING SupportedSymmetricCipherAlgorithms() := lib_cryptolib.CryptoLib.SupportedSymmetricCipherAlgorithms();


/**
 * Returns set of supported Public Key Algorithms
 *
 * @return        SET OF STRING containing all supported Public Key Algorithms
 */
EXPORT SET OF STRING SupportedPublicKeyAlgorithms() := lib_cryptolib.CryptoLib.SupportedPublicKeyAlgorithms();



/**
 * Hashing module containing all the supported hashing functions.
 *
 * @param   hashAlgorithm  The Hashing algorithm to use, as returned by SupportedHashAlgorithms()
 */
EXPORT Hashing(VARSTRING hashAlgorithm) := MODULE
    /**
     * Create a hash of the given data, using a hash algorithm that
     * was returned by SupportedHashAlgorithms()
     *
     * @param   inputData      Data to hash
     * @return                 Hashed contents
     */
    EXPORT DATA Hash(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Hash(hashAlgorithm, inputData);
    END;
END; // Hashing module

//-----

/**
 * Encryption module containing all symmetric encryption/decryption functions
 *
 * @param   algorithm      Symmetric algorithm to use, as returned by SupportedSymmetricCipherAlgorithms()
 * @param   passphrase     Passphrase string to use for encryption/encryption
 */
EXPORT SymmetricEncryption(VARSTRING algorithm, VARSTRING passphrase) := MODULE
    /**
     * Encrypt the given data, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   inputData  Contents to encrypt
     * @return             Encrypted cipher
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymmetricEncrypt( algorithm, passphrase, inputData );
    END;
    
    /**
     * Decrypt the given cipher, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   encryptedData  Contents to decrypt
     * @return                 Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymmetricDecrypt( algorithm, passphrase, encryptedData );
    END;
END; // SymmetricEncryption module

/**
 * Encryption module containing symmetric encryption/decryption functions
 *
 * @param   algorithm      Symmetric algorithm to use, as returned by SupportedSymmetricCipherAlgorithms()
 * @param   passphrase     Passphrase to use for encryption/encryption
 */
EXPORT SymmEncryption(VARSTRING algorithm, DATA passphrase) := MODULE
    /**
     * Encrypt the given data, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   inputData  Contents to encrypt
     * @return             Encrypted cipher
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymEncrypt( algorithm, passphrase, inputData );
    END;
    /**
     * Decrypt the given cipher, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   encryptedData  Contents to decrypt
     * @return                 Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymDecrypt( algorithm, passphrase, encryptedData );
    END;
END; // SymmEncryption module

/**
 * Encryption module containing asymmetric encryption/decryption/digital
 * signing/signature verification functions
 *
 * @param   pkAlgorithm    ASymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
 * @param   publicKeyFile  File specification of PEM formatted public key file
 * @param   privateKeyFile File specification of PEM formatted private key file
 * @param   passphrase     Passphrase string to use for encryption/encryption/signing/verifying
 */
EXPORT PublicKeyEncryption(VARSTRING pkAlgorithm, VARSTRING publicKeyFile = '', VARSTRING privateKeyFile = '', VARSTRING passphrase = '') := MODULE
    /**
     * Encrypt the given data, using the specified public key file,
     * passphrase, and algorithm
     *
     * @param   inputData    Contents to Encrypt
     * @return               Encrypted data
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Encrypt( pkAlgorithm, publicKeyFile, passphrase, inputData);
    END;

    /**
     * Decrypt the given encrypted data, using the specified private key file,
     * passphrase, and algorithm
     *
     * @param   encryptedData    Contents to Decrypt
     * @return                   Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Decrypt( pkAlgorithm, privateKeyFile, passphrase, encryptedData);
    END;

    /**
     * Create a digital signature of the given data, using the
     * specified private key file, passphrase and algorithm
     *
     * @param   inputData    Contents to sign
     * @return               Computed Digital signature
     */
    EXPORT DATA Sign( DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Sign( pkAlgorithm, privateKeyFile, passphrase, inputData);
    END;

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key file, passphrase and algorithm
     *
     * @param   signature      Signature to verify
     * @param   signedData     Data used to create signature
     * @return                 Boolean TRUE/FALSE
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.VerifySignature( pkAlgorithm, publicKeyFile, passphrase, signature, signedData);
    END;
END; // PublicKeyEncryption module

/**
 * Encryption module containing asymmetric encryption/decryption/digital
 * signing/signature verification functions
 *
 * @param   pkAlgorithm    ASymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
 * @param   publicKeyFile  File specification of PEM formatted public key file
 * @param   privateKeyFile File specification of PEM formatted private key file
 * @param   passphrase     Passphrase to use for encryption/decryption/signing/verifying
 */
EXPORT PKEncryption(VARSTRING pkAlgorithm, VARSTRING publicKeyFile = '', VARSTRING privateKeyFile = '', DATA passphrase = D'') := MODULE
    /**
     * Encrypt the given data, using the specified public key file,
     * passphrase, and algorithm
     *
     * @param   inputData    Contents to Encrypt
     * @return               Encrypted data
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKEncrypt( pkAlgorithm, publicKeyFile, passphrase, inputData);
    END;

    /**
     * Decrypt the given encrypted data, using the specified private key file,
     * passphrase, and algorithm
     *
     * @param   encryptedData    Contents to Decrypt
     * @return                   Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKDecrypt( pkAlgorithm, privateKeyFile, passphrase, encryptedData);
    END;

    /**
     * Create a digital signature of the given data, using the
     * specified private key file, passphrase and algorithm
     *
     * @param   inputData    Contents to sign
     * @return               Computed Digital signature
     */
    EXPORT DATA Sign( DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKSign( pkAlgorithm, privateKeyFile, passphrase, inputData);
    END;

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key file, passphrase and algorithm
     *
     * @param   signature      Signature to verify
     * @param   signedData     Data used to create signature
     * @return                 Boolean TRUE/FALSE
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKVerifySignature( pkAlgorithm, publicKeyFile, passphrase, signature, signedData);
    END;
END; // PKEncryption module

/**
 * Encryption module containing asymmetric encryption/decryption/digital
 * signing/signature verification functions
 *
 * @param   pkAlgorithm    Asymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
 * @param   publicKeyLFN   LFN specification of PEM formatted public key file
 * @param   privateKeyLFN  LFN specification of PEM formatted private key file
 * @param   passphrase     Passphrase string to use for encryption/encryption/signing/verifying
 */
EXPORT PublicKeyEncryptionFromLFN(VARSTRING pkAlgorithm, VARSTRING publicKeyLFN = '', VARSTRING privateKeyLFN = '', VARSTRING passphrase = '') := MODULE
    /**
     * Encrypt the given data, using the specified public key LFN,
     * passphrase, and algorithm
     *
     * @param   inputData    Contents to Encrypt
     * @return               Encrypted data
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.EncryptLFN( pkAlgorithm, publicKeyLFN, passphrase, inputData);
    END;

    /**
     * Decrypt the given encrypted data, using the specified private key LFN,
     * passphrase, and algorithm
     *
     * @param   encryptedData    Contents to Decrypt
     * @return                   Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.DecryptLFN( pkAlgorithm, privateKeyLFN, passphrase, encryptedData);
    END;

    /**
     * Create a digital signature of the given data, using the
     * specified private key LFN, passphrase and algorithm
     *
     * @param   inputData    Contents to sign
     * @return               Computed Digital signature
     */
    EXPORT DATA Sign( DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SignLFN( pkAlgorithm, privateKeyLFN, passphrase, inputData);
    END;

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key LFN, passphrase and algorithm
     *
     * @param   signature      Signature to verify
     * @param   signedData     Data used to create signature
     * @return                 Boolean TRUE/FALSE
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.VerifySignatureLFN( pkAlgorithm, publicKeyLFN, passphrase, signature, signedData);
    END;
END; // PublicKeyEncryptionFromLFN module


/**
 * Encryption module containing asymmetric encryption/decryption/digital
 * signing/signature verification functions
 *
 * @param   pkAlgorithm    Asymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
 * @param   publicKeyLFN   LFN specification of PEM formatted public key file
 * @param   privateKeyLFN  LFN specification of PEM formatted private key file
 * @param   passphrase     Passphrase to use for encryption/encryption/signing/verifying
 */
EXPORT PKEncryptionFromLFN(VARSTRING pkAlgorithm, VARSTRING publicKeyLFN = '', VARSTRING privateKeyLFN = '', DATA passphrase = D'') := MODULE
    /**
     * Encrypt the given data, using the specified public key LFN,
     * passphrase, and algorithm
     *
     * @param   inputData    Contents to Encrypt
     * @return               Encrypted data
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKEncryptLFN( pkAlgorithm, publicKeyLFN, passphrase, inputData);
    END;

    /**
     * Decrypt the given encrypted data, using the specified private key LFN,
     * passphrase, and algorithm
     *
     * @param   encryptedData    Contents to Decrypt
     * @return                   Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKDecryptLFN( pkAlgorithm, privateKeyLFN, passphrase, encryptedData);
    END;

    /**
     * Create a digital signature of the given data, using the
     * specified private key LFN, passphrase and algorithm
     *
     * @param   inputData    Contents to sign
     * @return               Computed Digital signature
     */
    EXPORT DATA Sign( DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKSignLFN( pkAlgorithm, privateKeyLFN, passphrase, inputData);
    END;

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key LFN, passphrase and algorithm
     *
     * @param   signature      Signature to verify
     * @param   signedData     Data used to create signature
     * @return                 Boolean TRUE/FALSE
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKVerifySignatureLFN( pkAlgorithm, publicKeyLFN, passphrase, signature, signedData);
    END;
END; // PKEncryptionFromLFN module

/**
  * Encryption module containing all asymmetric encryption/decryption/digital
  * signing/signature verification functions
  *
  * @param   pkAlgorithm    ASymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
  * @param   publicKeyBuff  PEM formatted Public key buffer
  * @param   privateKeyBuff PEM formatted Private key buffer
  * @param   passphrase     Passphrase string to use for encryption/encryption/signing/verifying
  */
EXPORT PublicKeyEncryptionFromBuffer(VARSTRING pkAlgorithm, VARSTRING publicKeyBuff = '', VARSTRING privateKeyBuff = '', VARSTRING passphrase = '') := MODULE
    /**
      * Encrypt the given data, using the specified public key, passphrase,
      * and algorithm
      *
      * @param   inputData    Contents to Encrypt
      * @return               Encrypted data
      */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.EncryptBuff( pkAlgorithm, publicKeyBuff, passphrase, inputData);
    END;
    
    /**
      * Decrypt the given data, using the specified private key, passphrase,
      * and algorithm
      *
      * @param   encryptedData  Contents to Decrypt
      * @return                 Decrypted data
      */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.DecryptBuff(pkAlgorithm, privateKeyBuff, passphrase, encryptedData);
    END;

    /**
      * Create a digital signature of the given data, using the specified private key,
      * passphrase, and algorithm
      *
      * @param   inputData    Contents to sign
      * @return               Computed digital signature
      */
    EXPORT DATA Sign(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SignBuff( pkAlgorithm, privateKeyBuff, passphrase, inputData);
    END;

    /**
      * Verify the given digital signature of the given data, using the specified public key,
      * passphrase, and algorithm
      *
      * @param   signature      Signature to verify     
      * @param   signedData     Data used to create signature
      * @return                 Booolean TRUE if signature is valid, otherwise FALSE
      */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.VerifySignatureBuff( pkAlgorithm, publicKeyBuff, passphrase, signature, signedData);
    END;
    
END; // PublicKeyEncryptionFromBuffer module

/**
  * Encryption module containing asymmetric encryption/decryption/digital
  * signing/signature verification functions
  *
  * @param   pkAlgorithm    ASymmetric algorithm to use, as returned by SupportedPublicKeyAlgorithms()
  * @param   publicKeyBuff  PEM formatted Public key buffer
  * @param   privateKeyBuff PEM formatted Private key buffer
  * @param   passphrase     Passphrase to use for encryption/encryption/signing/verifying
  */

EXPORT PKEncryptionFromBuffer(VARSTRING pkAlgorithm, VARSTRING publicKeyBuff = '', VARSTRING privateKeyBuff = '', DATA passphrase = D'') := MODULE
    /**
      * Encrypt the given data, using the specified public key, passphrase,
      * and algorithm
      *
      * @param   inputData    Contents to Encrypt
      * @return               Encrypted data
      */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKEncryptBuff( pkAlgorithm, publicKeyBuff, passphrase, inputData);
    END;

    /**
      * Decrypt the given data, using the specified private key, passphrase,
      * and algorithm
      *
      * @param   encryptedData  Contents to Decrypt
      * @return                 Decrypted data
      */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKDecryptBuff(pkAlgorithm, privateKeyBuff, passphrase, encryptedData);
    END;

    /**
      * Create a digital signature of the given data, using the specified private key,
      * passphrase, and algorithm
      *
      * @param   inputData    Contents to sign
      * @return               Computed digital signature
      */
    EXPORT DATA Sign(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKSignBuff( pkAlgorithm, privateKeyBuff, passphrase, inputData);
    END;

    /**
      * Verify the given digital signature of the given data, using the specified public key,
      * passphrase, and algorithm
      *
      * @param   signature      Signature to verify
      * @param   signedData     Data used to create signature
      * @return                 Booolean TRUE if signature is valid, otherwise FALSE
      */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.PKVerifySignatureBuff( pkAlgorithm, publicKeyBuff, passphrase, signature, signedData);
    END;
END; //PKEncryptionFromBuffer module

END; // Crypto module
