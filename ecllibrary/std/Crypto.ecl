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
 * Hashing module that can be instantiated by an ECL caller.  Data can be hashed
 * with functionality here
 *
 * @param   hashAlgorithm  Hashing algorithm to be used, as returned by SupportedHashAlgorithms()
 * @return                 Hashing object reference
 */
EXPORT Hashing(VARSTRING hashAlgorithm) := MODULE
    /**
     * Create a hash of the given data, using a hash algorithm that
     * was returned by SupportedHashAlgorithms()
     *
     * @param   inputData      Data to be hashed
     * @return                 Hashed contents
     */
    EXPORT DATA Hash(DATA inputData) := FUNCTION
        return lib_cryptolib.CryptoLib.Hash(hashAlgorithm, inputData);
    END;
END; // Hashing module

//-----

/**
 * Encryption module that can be instantiated by an ECL caller
 * to perform symmetric encryption/decryption
 *
 * @param   algorithm      Symmetric algorithm to be used, as returned by SupportedSymmetricCipherAlgorithms()
 * @param   passphrase     Passphrase to be used for encryption/encryption
 * @return                 Encryption object reference
 */
EXPORT SymmetricEncryption(VARSTRING algorithm, VARSTRING passphrase) := MODULE
    /**
     * Encrypt the given data, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   inputData  Contents to be encrypted
     * @return             Encrypted cipher
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymmetricEncrypt( algorithm, passphrase, inputData );
    END;
    
    /**
     * Decrypt the given cipher, using the specified passphrase and symmetric cipher
     * algorithm that was returned by SupportedSymmetricCipherAlgorithms()
     *
     * @param   encryptedData  Contents to be decrypted
     * @return                 Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SymmetricDecrypt( algorithm, passphrase, encryptedData );
    END;
END; // SymmetricEncryption module




/**
 * Encryption module that can be instantiated by an ECL caller
 * to perform asymmetric encryption/decryption/digital signing/signature verification
 *
 * @param   pkAlgorithm    ASymmetric algorithm to be used, as returned by SupportedPublicKeyAlgorithms()
 * @param   publicKeyFile  File specification of PEM formatted public key file
 * @param   privateKeyFile File specification of PEM formatted private key file
 * @param   passphrase	   Passphrase to be used for encryption/encryption/signing/verifying
 * @return                 Encryption object reference
 */
EXPORT PublicKeyEncryption(VARSTRING pkAlgorithm, VARSTRING publicKeyFile = '', VARSTRING privateKeyFile = '', VARSTRING passphrase = '') := MODULE
    /**
     * Encrypt the given data, using the specified public key file,
     * passphrase, and algorithm
     *
     * @param   inputData    Contents to be Encrypted
     * @return               Encrypted data
     */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Encrypt( pkAlgorithm, publicKeyFile, passphrase, inputData);
    END;

    /**
     * Decrypt the given encrypted data, using the specified private key file,
     * passphrase, and algorithm
     *
     * @param   encryptedData    Contents to be Decrypted
     * @return                   Decrypted data
     */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Decrypt( pkAlgorithm, privateKeyFile, passphrase, encryptedData);
    END;

    /**
     * Create a digital signature of the given data, using the
     * specified private key file, passphrase and algorithm
     *
     * @param   inputData    Contents to be signed
     * @return               Computed Digital signature
     */
    EXPORT DATA Sign( DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.Sign( pkAlgorithm, privateKeyFile, passphrase, inputData);
    END;

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key file, passphrase and algorithm
     *
     * @param   signature      Signature to be verified
     * @param   signedData     Data used to create signature
     * @return                 BOOL
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.VerifySignature( pkAlgorithm, publicKeyFile, passphrase, signature, signedData);
    END;
END; // PublicKeyEncryption module



/**
  * Encryption module that can be instantiated by an ECL caller
  * to perform asymmetric encryption/decryption/digital signing/signature verification
  *
  * @param   pkAlgorithm    ASymmetric algorithm to be used, as returned by SupportedPublicKeyAlgorithms()
  * @param   publicKeyBuff  PEM formatted Public key buffer
  * @param   privateKeyBuff PEM formatted Private key buffer
  * @param   passphrase     Passphrase to be used for encryption/encryption/signing/verifying
  * @return                 Encryption object reference
  */
EXPORT PublicKeyEncryptionFromBuffer(VARSTRING pkAlgorithm, VARSTRING publicKeyBuff = '', VARSTRING privateKeyBuff = '', VARSTRING passphrase = '') := MODULE
    /**
      * Encrypt the given data, using the specified public key, passphrase,
      * and algorithm
      *
      * @param   inputData    Contents to be Encrypted
      * @return               Encrypted data
      */
    EXPORT DATA Encrypt(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.EncryptBuff( pkAlgorithm, publicKeyBuff, passphrase, inputData);
    END;
    
    /**
      * Decrypt the given data, using the specified private key, passphrase,
      * and algorithm
      *
      * @param   encryptedData  Contents to be Decrypted
      * @return                 Decrypted data
      */
    EXPORT DATA Decrypt(DATA encryptedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.DecryptBuff(pkAlgorithm, privateKeyBuff, passphrase, encryptedData);
    END;

    /**
      * Create a digital signature of the given data, using the specified private key,
      * passphrase, and algorithm
      *
      * @param   inputData    Contents to be signed
      * @return               Computed digital signature
      */
    EXPORT DATA Sign(DATA inputData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.SignBuff( pkAlgorithm, privateKeyBuff, passphrase, inputData);
    END;

    /**
      * Verify the given digital signature of the given data, using the specified public key,
      * passphrase, and algorithm
      *
      * @param   signature      Signature to be verified     
      * @param   signedData     Data used to create signature
      * @return                 True if signature is valid, false otherwise
      */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData) := FUNCTION
        RETURN lib_cryptolib.CryptoLib.VerifySignatureBuff( pkAlgorithm, publicKeyBuff, passphrase, signature, signedData);
    END;
    
END; // PublicKeyEncryption module


END; // Crypto module
