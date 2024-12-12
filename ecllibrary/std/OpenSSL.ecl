/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the License);
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an AS IS BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

EXPORT OpenSSL := MODULE

IMPORT lib_sslservices;

EXPORT Digest := MODULE

    /**
     * Returns a list of the names of the available hash digest algorithms.
     *
     * This is primarily an introspection/discovery function. Once
     * you determine the algorithm you want to use, you should hardcode it.
     *
     * @return  A dataset containing the hash algorithm names.
     *
     * @see     Hash()
     *          PublicKey.Sign()
     *          PublicKey.VerifySignature()
     */
    EXPORT DATASET({STRING name}) AvailableAlgorithms() := lib_sslservices.SSLServices.digestAvailableAlgorithms();

    /**
     * Compute the hash of given data according to the named
     * hash algorithm.
     *
     * @param   indata           The data to hash; REQUIRED
     * @param   algorithm_name   The name of the hash algorithm to use;
     *                           must be one of the values returned from
     *                           the AvailableAlgorithms() function in
     *                           this module; cannot be empty; REQUIRED
     *
     * @return  A DATA value representing the hash value of indata.
     *
     * @see     AvailableAlgorithms()
     */
    EXPORT DATA Hash(DATA indata, VARSTRING algorithm_name) := lib_sslservices.SSLServices.digesthash(indata, algorithm_name);

END; // Digest

EXPORT Ciphers := MODULE

    /**
     * Returns a list of the names of the available symmetric
     * cipher algorithms.
     *
     * This is primarily an introspection/discovery function. Once
     * you determine the algorithm you want to use, you should hardcode it.
     *
     * @return  A dataset containing the symmetric cipher algorithm names.
     *
     * @see     IVSize()
     *          SaltSize()
     *          Encrypt()
     *          Decrypt()
     */
    EXPORT DATASET({STRING name}) AvailableAlgorithms() := lib_sslservices.SSLServices.cipherAvailableAlgorithms();

    /**
     * Return the size of the IV used for the given symmetric
     * cipher algorithm.
     *
     * This is primarily an introspection/discovery function. Once
     * you determine the proper value for the algorithm you want to
     * use, you should hardcode it.
     *
     * @param   algorithm_name   The name of the symmetric cipher to examine;
     *                           must be one of the values returned from
     *                           the AvailableAlgorithms() function in
     *                           this module; cannot be empty; REQUIRED
     *
     * @return  The size of the IV used by the given algorithm, in bytes.
     *
     * @see     AvailableAlgorithms()
     */
    EXPORT UNSIGNED2 IVSize(VARSTRING algorithm_name) := lib_sslservices.SSLServices.cipherIVSize(algorithm_name);

    /**
     * Return the size of the salt used for the given symmetric
     * cipher algorithm.
     *
     * This is primarily an introspection/discovery function. Once
     * you determine the proper value for the algorithm you want to
     * use, you should hardcode it.
     *
     * @param   algorithm_name   The name of the symmetric cipher to examine;
     *                           must be one of the values returned from
     *                           the AvailableAlgorithms() function in
     *                           this module; cannot be empty; REQUIRED
     *
     * @return  The size of the salt used by the given algorithm, in bytes.
     *
     * @see     AvailableAlgorithms()
     */
    EXPORT UNSIGNED2 SaltSize(VARSTRING algorithm_name) := 8;

    /**
     * Encrypt some plaintext with the given symmetric cipher and a
     * passphrase. Optionally, you can specify static IV and salt values.
     * The encrypted ciphertext is returned as a DATA value.
     *
     * If IV or salt values are explicitly provided during encryption then
     * those same values must be provided during decryption.
     *
     * @param   plaintext       The data to encrypt; REQUIRED
     * @param   algorithm_name  The name of the symmetric cipher to use;
     *                          must be one of the values returned from
     *                          the AvailableAlgorithms() function in
     *                          this module; cannot be empty; REQUIRED
     * @param   passphrase      Passphrase to use for private key;
     *                          If no passphrase was used when generating
     *                          the private key, an empty string must be
     *                          passed in (e.g. (DATA)''); REQUIRED
     * @param   iv              The IV to use during encryption; if not set
     *                          then a random value will be generated; if set,
     *                          it must be of the expected size for the given
     *                          algorithm; OPTIONAL, defaults to creating a
     *                          random value
     * @param   salt            The salt to use during encryption; if not set
     *                          then a random value will be generated; if set,
     *                          it must be of the expected size for the given
     *                          algorithm; OPTIONAL, defaults to creating a
     *                          random value
     *
     * @return  The ciphertext as a DATA type.
     *
     * @see     AvailableAlgorithms()
     *          IVSize()
     *          SaltSize()
     *          Decrypt()
     */
    EXPORT DATA Encrypt(DATA plaintext, VARSTRING algorithm_name, DATA passphrase, DATA iv = (DATA)'', DATA salt = (DATA)'') := lib_sslservices.SSLServices.cipherEncrypt(plaintext, algorithm_name, passphrase, iv, salt);


    /**
     * Decrypt some ciphertext with the given symmetric cipher and a
     * passphrase. Optionally, you can specify static IV and salt values.
     * The decrypted plaintext is returned as a DATA value.
     *
     * @param   ciphertext      The data to decrypt; REQUIRED
     * @param   algorithm_name  The name of the symmetric cipher to use;
     *                          must be one of the values returned from
     *                          the AvailableAlgorithms() function in
     *                          this module; cannot be empty; REQUIRED
     * @param   passphrase      Passphrase to use for private key;
     *                          If no passphrase was used when generating
     *                          the private key, an empty string must be
     *                          passed in (e.g. (DATA)''); REQUIRED
     * @param   iv              The IV to use during decryption; if not set
     *                          then a random value will be used; if set,
     *                          it must be of the expected size for the given
     *                          algorithm; OPTIONAL, defaults to creating a
     *                          random value
     * @param   salt            The salt to use during decryption; if not set
     *                          then a random value will be used; if set,
     *                          it must be of the expected size for the given
     *                          algorithm; OPTIONAL, defaults to creating a
     *                          random value
     *
     * @return  The plaintext as a DATA type.
     *
     * @see     AvailableAlgorithms()
     *          IVSize()
     *          SaltSize()
     *          Encrypt()
     */
    EXPORT DATA Decrypt(DATA ciphertext, VARSTRING algorithm_name, DATA passphrase, DATA iv = (DATA)'', DATA salt = (DATA)'') := lib_sslservices.SSLServices.cipherDecrypt(ciphertext, algorithm_name, passphrase, iv, salt);
END; // Ciphers

EXPORT PublicKey := MODULE

    /**
     * Perform a hybrid encryption using one or more RSA public keys.
     *
     * Because asymmetric encryption is computationally expensive, large
     * payloads are actually encrypted with a symmetric cipher and a
     * randomly-generated passphrase. The passphrase, which is much shorter,
     * is then encrypted with the public key. The whole package is then bundled
     * together into an "envelope" and "sealed".
     *
     * The function uses RSA public keys and they must be in PEM format. To
     * generate such keys on the command line:
     *
     *      ssh-keygen -b 4096 -t rsa -m pem -f sample2
     *      ssh-keygen -f sample2 -e -m pem > sample2.pub
     *
     * The resulting files, sample2 and sample2.pub, are the private and public
     * keys, respectively. Their contents may be passed to this function.
     *
     * @param   plaintext           The data to encrypt; REQUIRED
     * @param   pem_public_keys     One or more RSA public keys, in PEM format;
     *                              note that this is a SET -- you can pass
     *                              more than one public key here, and the resulting
     *                              ciphertext can be decrypted by any one of the
     *                              corresponding private keys; REQUIRED
     * @param   algorithm_name      The name of the symmetric algorithm to use
     *                              to encrypt the payload; must be one of those
     *                              returned by Ciphers.AvailableAlgorithms();
     *                              OPTIONAL, defaults to aes-256-cbc
     *
     * @return  The encrypted ciphertext.
     *
     * @see     RSAUnseal()
     *          Ciphers.AvailableAlgorithms()
     */
    EXPORT DATA RSASeal(DATA plaintext, SET OF STRING pem_public_keys, VARSTRING algorithm_name = 'aes-256-cbc') := lib_sslservices.SSLServices.pkRSASeal(plaintext, pem_public_keys, algorithm_name);

    /**
     * Decrypts ciphertext previously generated by the RSASeal() function.
     *
     * Because asymmetric encryption is computationally expensive, large
     * payloads are actually encrypted with a symmetric cipher and a
     * randomly-generated passphrase. The passphrase, which is much shorter,
     * is then encrypted with the public key. The whole package is then bundled
     * together into an "envelope" and "sealed". Given the private key that
     * corresponds to one of the public keys used to create the ciphertext,
     * this function unpacks everything and decrypts the payload.
     *
     * The function uses RSA public keys and they must be in PEM format. To
     * generate such keys on the command line:
     *
     *      ssh-keygen -b 4096 -t rsa -m pem -f sample2
     *      ssh-keygen -f sample2 -e -m pem > sample2.pub
     *
     * The resulting files, sample2 and sample2.pub, are the private and public
     * keys, respectively. Their contents may be passed to this function.
     *
     * @param   ciphertext          The data to decrypt; REQUIRED
     * @param   passphrase          Passphrase to use for private key;
     *                              If no passphrase was used when generating
     *                              the private key, an empty string must be
     *                              passed in (e.g. (DATA)''); REQUIRED
     * @param   pem_private_key     An RSA public key in PEM format; REQUIRED
     * @param   algorithm_name      The name of the symmetric algorithm to use
     *                              to decrypt the payload; must be one of those
     *                              returned by Ciphers.AvailableAlgorithms() and
     *                              it must match the algorithm used to create the
     *                              ciphertext; OPTIONAL, defaults to aes-256-cbc
     *
     * @return  The decrypted plaintext.
     *
     * @see     RSASeal()
     *          Ciphers.AvailableAlgorithms()
     */
    EXPORT DATA RSAUnseal(DATA ciphertext, DATA passphrase, STRING pem_private_key, VARSTRING algorithm_name = 'aes-256-cbc') := lib_sslservices.SSLServices.pkRSAUnseal(ciphertext, passphrase, pem_private_key, algorithm_name);

    /**
     * This function performs asymmetric encryption. It should be used to
     * encrypt only small plaintext (e.g. less than 100 bytes) because it is
     * computationally expensive.
     *
     * @param   plaintext       The data to encrypt; REQUIRED
     * @param   pem_public_key  The public key to use for encryption, in
     *                          PEM format; REQUIRED
     *
     * @return  The encrypted ciphertext.
     *
     * @see     Decrypt()
     */
    EXPORT DATA Encrypt(DATA plaintext, STRING pem_public_key) := lib_sslservices.SSLServices.pkEncrypt(plaintext, pem_public_key);

    /**
     * This function performs asymmetric decryption. It should be used to
     * decrypt only small plaintext (e.g. less than 100 bytes) because it is
     * computationally expensive.
     *
     * @param   ciphertext      The data to decrypt; REQUIRED
     * @param   passphrase      Passphrase to use for private key;
     *                          If no passphrase was used when generating
     *                          the private key, an empty string must be
     *                          passed in (e.g. (DATA)''); REQUIRED
     * @param   pem_private_key The private key to use for decryption, in
     *                          PEM format; REQUIRED
     *
     * @return  The decrypted plaintext.
     *
     * @see     Encrypt()
     */
    EXPORT DATA Decrypt(DATA ciphertext, DATA passphrase, STRING pem_private_key) := lib_sslservices.SSLServices.pkDecrypt(ciphertext, passphrase, pem_private_key);

    /**
     * Create a digital signature of the given data, using the
     * specified private key, passphrase and algorithm.
     *
     * The function uses an RSA private key and it must be in PEM format. To
     * generate such keys on the command line:
     *
     *      ssh-keygen -b 4096 -t rsa -m pem -f sample2
     *      ssh-keygen -f sample2 -e -m pem > sample2.pub
     *
     * The resulting files, sample2 and sample2.pub, are the private and public
     * keys, respectively. Their contents may be passed to this function
     *
     * @param   plaintext       Contents to sign; REQUIRED
     * @param   passphrase      Passphrase to use for private key;
     *                          If no passphrase was used when generating
     *                          the private key, an empty string must be
     *                          passed in (e.g. (DATA)''); REQUIRED
     * @param   pem_private_key Private key to use for signing; REQUIRED
     * @param   algorithm_name  The name of the hash algorithm to use;
     *                          must be one of the values returned from
     *                          the AvailableAlgorithms() function in the
     *                          Digest module; OPTIONAL, defaults to sha256
     * @return                  Computed Digital signature
     *
     * @see     Digest.AvailableAlgorithms()
     *          VerifySignature()
     */
    EXPORT DATA Sign(DATA plaintext, DATA passphrase, STRING pem_private_key, VARSTRING algorithm_name = 'sha256') := lib_sslservices.SSLServices.pkSign(plaintext, passphrase, pem_private_key, algorithm_name);

    /**
     * Verify the given digital signature of the given data, using
     * the specified public key, passphrase and algorithm.
     *
     * The function uses an RSA public key and it must be in PEM format. To
     * generate such keys on the command line:
     *
     *      ssh-keygen -b 4096 -t rsa -m pem -f sample2
     *      ssh-keygen -f sample2 -e -m pem > sample2.pub
     *
     * The resulting files, sample2 and sample2.pub, are the private and public
     * keys, respectively. Their contents may be passed to this function
     *
     * @param   signature      Signature to verify; REQUIRED
     * @param   signedData     Data used to create signature; REQUIRED
     * @param   pem_public_key Public key to use for verification; REQUIRED
     * @param   algorithm_name The name of the hash algorithm to use;
     *                         must be one of the values returned from
     *                         the AvailableAlgorithms() function in the
     *                         Digest module; OPTIONAL, defaults to sha256
     * @return                 Boolean TRUE/FALSE
     *
     * @see     Digest.AvailableAlgorithms()
     *          Sign()
     */
    EXPORT BOOLEAN VerifySignature(DATA signature, DATA signedData, STRING pem_public_key, VARSTRING algorithm_name = 'sha256') := lib_sslservices.SSLServices.pkVerifySignature(signature, signedData, pem_public_key, algorithm_name);

END; // PublicKey

END;
