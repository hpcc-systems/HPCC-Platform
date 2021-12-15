/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;

/**
 * A specific instance of this module needs connection and user credential
 * information, provided as arguments.
 *
 * @param   username        The username of the user requesting access to the
 *                          key value store; this is typically the same
 *                          username used to login to ECL Watch; set to an
 *                          empty string if authentication is not required;
 *                          OPTIONAL, defaults to an empty string
 * @param   userPW          The password of the user requesting access to the
 *                          key value store; this is typically the same
 *                          password used to login to ECL Watch; set to an
 *                          empty string if authentication is not required;
 *                          OPTIONAL, defaults to an empty string
 * @param   espURL          The full URL for accessing the esp process running
 *                          on the HPCC Systems cluster (this is typically the
 *                          same URL as used to access ECL Watch); set to an
 *                          empty string to use the URL of the current esp
 *                          process as found via Std.File.GetEspURL();
 *                          OPTIONAL, defaults to an empty string
 *
 * @return  A reference to the module, correctly initialized with the
 *          given access parameters.
 */
EXPORT Store(STRING username = '',
             STRING userPW = '',
             STRING espURL = '') := MODULE

    SHARED MY_USERNAME := TRIM(username, ALL);
    SHARED MY_USER_PW := TRIM(userPW, LEFT, RIGHT);
    SHARED ENCODED_CREDENTIALS := IF
        (
            MY_USERNAME != '',
            'Basic ' + Std.Str.EncodeBase64((DATA)(MY_USERNAME + ':' + MY_USER_PW)),
            ''
        );

    // The URL that will be used by all SOAPCALL invocations
    TRIMMED_URL := TRIM(espURL, ALL);
    SHARED MY_ESP_URL := IF(TRIMMED_URL != '', TRIMMED_URL, Std.File.GetEspURL(MY_USERNAME, MY_USER_PW)) + '/WsStore/?ver_=1.02';

    /**
     * Helper for function for setting the has_exception field within a
     * response record.
     *
     * @param   rec     The single RECORD containing a WsStore response
     *
     * @return  The same record with the has_exceptions field set to TRUE
     *          or FALSE based on examining the exception list.
     */
    SHARED UpdateForExceptions(res) := FUNCTIONMACRO
        RETURN PROJECT
            (
                res,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.has_exceptions := EXISTS(LEFT.exceptions.exceptions),
                        SELF := LEFT
                    )
            );
    ENDMACRO;

    //--------------------------------------------------------------------------
    // Exported record definitions; all used as results of SOAPCALL invocations
    //--------------------------------------------------------------------------

    EXPORT ExceptionLayout := RECORD
        STRING                          code            {XPATH('Code')};
        STRING                          audience        {XPATH('Audience')};
        STRING                          source          {XPATH('Source')};
        STRING                          message         {XPATH('Message')};
    END;

    EXPORT ExceptionListLayout := RECORD
        STRING                          source          {XPATH('Source')};
        DATASET(ExceptionLayout)        exceptions      {XPATH('Exception')};
    END;

    EXPORT StoreInfoRec := RECORD
        STRING                          store_name      {XPATH('Name')};
        STRING                          description     {XPATH('Description')};
        STRING                          owner           {XPATH('Owner')};
        STRING                          create_time     {XPATH('CreateTime')};
        UNSIGNED4                       max_value_size  {XPATH('MaxValSize')};
        BOOLEAN                         is_default      {XPATH('IsDefault')};
    END;

    EXPORT ListStoresResponseRec := RECORD
        DATASET(StoreInfoRec)           stores          {XPATH('Stores/Store')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    EXPORT CreateStoreResponseRec := RECORD
        BOOLEAN                         succeeded       {XPATH('Success')};     // Will be TRUE if a new store was created, FALSE if store already existed or an error occurred
        BOOLEAN                         already_present := FALSE;               // Will be TRUE if store already existed
        STRING                          store_name      {XPATH('Name')};
        STRING                          description     {XPATH('Description')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    EXPORT SetKeyValueResponseRec := RECORD
        BOOLEAN                         succeeded       {XPATH('Success')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    EXPORT GetKeyValueResponseRec := RECORD
        BOOLEAN                         was_found := TRUE;                      // Will be FALSE if key was not found
        STRING                          value           {XPATH('Value')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    EXPORT DeleteKeyValueResponseRec := RECORD
        BOOLEAN                         succeeded       {XPATH('Success')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    SHARED KeySetRec := RECORD
        STRING                          key             {XPATH('')};
    END;

    EXPORT GetAllKeysResponseRec := RECORD
        STRING                          namespace       {XPATH('Namespace')};
        DATASET(KeySetRec)              keys            {XPATH('KeySet/Key')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    SHARED KeyValueRec := RECORD
        STRING                          key             {XPATH('Key')};
        STRING                          value           {XPATH('Value')};
    END;

    EXPORT GetAllKeyValuesResponseRec := RECORD
        STRING                          namespace       {XPATH('Namespace')};
        DATASET(KeyValueRec)            key_values      {XPATH('Pairs/Pair')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    SHARED NamespaceLayout := RECORD
        STRING                          namespace       {XPATH('')};
    END;

    EXPORT ListNamespacesResponseRec := RECORD
        DATASET(NamespaceLayout)        namespaces      {XPATH('Namespaces/Namespace')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    EXPORT DeleteNamespaceResponseRec := RECORD
        BOOLEAN                         succeeded       {XPATH('Success')};
        BOOLEAN                         has_exceptions := FALSE;
        ExceptionListLayout             exceptions      {XPATH('Exceptions')};
    END;

    //--------------------------------------------------------------------------

    /**
     * Creates a key/value store if it has not been created before.  If the
     * store has already been created then this function has no effect.
     *
     * @param   storeName           A STRING naming the store to create; this
     *                              cannot be an empty string; REQUIRED
     * @param   description         A STRING describing the purpose of the
     *                              store; may be an empty string; OPTIONAL,
     *                              defaults to an empty string
     * @param   maxValueSize        The maximum size of any value stored within
     *                              this store, in bytes; use a value of zero to
     *                              indicate an unlimited maximum size; OPTIONAL,
     *                              defaults to 1024
     * @param   isUserSpecific      If TRUE, this store will be visible only
     *                              to the user indicated by the (username, userPW)
     *                              arguments provided when the module was
     *                              defined; if FALSE, the store will be global
     *                              and visible to all users; OPTIONAL, defaults
     *                              to FALSE
     * @param   timeoutInSeconds    The number of seconds to wait for the
     *                              underlying SOAPCALL to complete; set to zero
     *                              to wait forever; OPTIONAL, defaults to zero
     *
     * @return  A CreateStoreResponseRec RECORD.  If the store already exists
     *          then the result will show succeeded = FALSE and
     *          already_present = TRUE.
     */
    EXPORT CreateStore(STRING storeName,
                       STRING description = '',
                       UNSIGNED4 maxValueSize = 1024,
                       BOOLEAN isUserSpecific = FALSE,
                       UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
        soapResponse := SOAPCALL
            (
                MY_ESP_URL,
                'CreateStore',
                {
                    STRING      pStoreName      {XPATH('Name')} := storeName;
                    STRING      pDescription    {XPATH('Description')} := description;
                    UNSIGNED4   pMaxValueSize   {XPATH('MaxValueSize')} := maxValueSize;
                    BOOLEAN     pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                },
                DATASET(CreateStoreResponseRec),
                XPATH('CreateStoreResponse'),
                HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                TIMEOUT(timeoutInSeconds)
            );

        finalResponse := PROJECT
            (
                soapResponse,
                TRANSFORM
                    (
                        RECORDOF(LEFT),
                        SELF.already_present := (NOT LEFT.succeeded) AND (NOT EXISTS(LEFT.exceptions.exceptions)),
                        SELF := LEFT
                    )
            );

        RETURN UpdateForExceptions(finalResponse)[1];
    END;

    /**
     * Gets a list of available stores.
     *
     * @param   nameFilter          A STRING defining a filter to be applied
     *                              to the store's name; the filter accepts
     *                              the '*' wildcard character to indicate
     *                              'match anything' and '?' to match any
     *                              single character; string comparisons are
     *                              case-insensitive; an empty string is
     *                              equivalent to '*'; OPTIONAL, defaults
     *                              to '*'
     * @param   ownerFilter         A STRING defining a filter to be applied
     *                              to the store's owner; the filter accepts
     *                              the '*' wildcard character to indicate
     *                              'match anything' and '?' to match any
     *                              single character; string comparisons are
     *                              case-insensitive; an empty string is
     *                              equivalent to '*'; OPTIONAL, defaults
     *                              to '*'
     * @param   timeoutInSeconds    The number of seconds to wait for the
     *                              underlying SOAPCALL to complete; set to zero
     *                              to wait forever; OPTIONAL, defaults to zero
     *
     * @return  A ListStoresResponseRec RECORD.
     */
    EXPORT ListStores(STRING nameFilter = '*',
                      STRING ownerFilter = '*',
                      UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
        soapResponse := SOAPCALL
            (
                MY_ESP_URL,
                'ListStores',
                {
                    STRING  pNameFilter     {XPATH('NameFilter')} := nameFilter;
                    STRING  pOwnerFilter    {XPATH('OwnerFilter')} := ownerFilter;
                },
                DATASET(ListStoresResponseRec),
                XPATH('ListStoresResponse'),
                HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                TIMEOUT(timeoutInSeconds)
            );

        RETURN UpdateForExceptions(soapResponse)[1];
    END;

    /**
     * Gets a list of namespaces defined in the current store.
     *
     * @param   storeName           A STRING naming the store containing the
     *                              namespaces you are interested in; set this
     *                              to an empty string to reference the default
     *                              store in the cluster, if one has been
     *                              defined; REQUIRED
     * @param   isUserSpecific      If TRUE, the system will look only for
     *                              private keys; if FALSE then the system will
     *                              look for global keys; OPTIONAL, defaults
     *                              to FALSE
     * @param   timeoutInSeconds    The number of seconds to wait for the
     *                              underlying SOAPCALL to complete; set to zero
     *                              to wait forever; OPTIONAL, defaults to zero
     *
     * @return  A ListNamespacesResponseRec RECORD.
     */
    EXPORT ListNamespaces(STRING storeName,
                          BOOLEAN isUserSpecific = FALSE,
                          UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
        soapResponse := SOAPCALL
            (
                MY_ESP_URL,
                'ListNamespaces',
                {
                    STRING  pStoreName      {XPATH('StoreName')} := storeName;
                    BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                },
                DATASET(ListNamespacesResponseRec),
                XPATH('ListNamespacesResponse'),
                HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                TIMEOUT(timeoutInSeconds)
            );

        RETURN UpdateForExceptions(soapResponse)[1];
    END;

    /**
     * This submodule nails down a specific namespace to access within a
     * key/value store on the cluster
     *
     * @param   namespace       A STRING naming the namespace partition that will
     *                          be used for this module; cannot be an empty string;
     *                          REQUIRED
     * @param   storeName       A STRING naming the store that this module
     *                          will access; set this to an empty string to
     *                          reference the default store in the cluster, if
     *                          one has been defined; OPTIONAL, defaults to
     *                          an empty string
     *
     * @return  A reference to the module, correctly initialized with the
     *          given namespace.
     */
    EXPORT WithNamespace(STRING namespace,
                         STRING storeName = '') := MODULE

        /**
         * Sets a value for a key within a namespace.  If the key already exists
         * then its value is overridden.  The namespace will be created if it has
         * not already been defined.
         *
         * @param   keyName             A STRING naming the key; may not be an
         *                              empty string; REQUIRED
         * @param   keyValue            A STRING representing the value to store
         *                              for the key; may be an empty string;
         *                              REQUIRED
         * @param   isUserSpecific      If TRUE, this key will be visible only
         *                              to the user indicated by the (username, userPW)
         *                              arguments provided when the module was
         *                              defined; if FALSE, the key will be global
         *                              and visible to all users; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A SetKeyValueResponseRec RECORD.
         */
        EXPORT SetKeyValue(STRING keyName,
                           STRING keyValue,
                           BOOLEAN isUserSpecific = FALSE,
                           UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'Set',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        STRING  pKey            {XPATH('Key')} := keyName;
                        STRING  pValue          {XPATH('Value')} := keyValue;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(SetKeyValueResponseRec),
                    XPATH('SetResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            RETURN UpdateForExceptions(soapResponse)[1];
        END;

        /**
         * Gets a previously-set value for a key within a namespace.
         *
         * @param   keyName             A STRING naming the key; may not be an
         *                              empty string; REQUIRED
         * @param   isUserSpecific      If TRUE, the system will look only for
         *                              private keys; if FALSE then the system will
         *                              look for global keys; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A GetKeyValueResponseRec RECORD.  Note that the record will have
         *          was_found set to TRUE or FALSE, depending on whether the key
         *          was actually found in the key/value store.
         */
        EXPORT GetKeyValue(STRING keyName,
                           BOOLEAN isUserSpecific = FALSE,
                           UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'Fetch',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        STRING  pKey            {XPATH('Key')} := keyName;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(GetKeyValueResponseRec),
                    XPATH('FetchResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            finalResponse := PROJECT
                (
                    soapResponse,
                    TRANSFORM
                        (
                            RECORDOF(LEFT),
                            SELF.was_found := LEFT.value != '' AND NOT EXISTS(LEFT.exceptions.exceptions),
                            SELF := LEFT
                        )
                );

            RETURN UpdateForExceptions(finalResponse)[1];
        END;

        /**
         * Deletes a previously-set key and value within a namespace.
         *
         * @param   keyName             A STRING naming the key; may not be an
         *                              empty string; REQUIRED
         * @param   isUserSpecific      If TRUE, the system will look only for
         *                              private keys; if FALSE then the system will
         *                              look for global keys; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A DeleteKeyValueResponseRec RECORD.
         */
        EXPORT DeleteKeyValue(STRING keyName,
                              BOOLEAN isUserSpecific = FALSE,
                              UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'Delete',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        STRING  pKey            {XPATH('Key')} := keyName;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(DeleteKeyValueResponseRec),
                    XPATH('DeleteResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            RETURN UpdateForExceptions(soapResponse)[1];
        END;

        /**
         * Gets a list of all keys currently defined within a namespace.
         *
         * @param   isUserSpecific      If TRUE, the system will look only for
         *                              private keys; if FALSE then the system will
         *                              look for global keys; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A GetAllKeysResponseRec RECORD.
         */
        EXPORT GetAllKeys(BOOLEAN isUserSpecific = FALSE,
                          UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'ListKeys',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(GetAllKeysResponseRec),
                    XPATH('ListKeysResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            RETURN UpdateForExceptions(soapResponse)[1];
        END;

        /**
         * Gets a list of all key and their associated values currently defined
         * within a namespace.
         *
         * @param   isUserSpecific      If TRUE, the system will look only for
         *                              private keys; if FALSE then the system will
         *                              look for global keys; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A GetAllKeyValuesResponseRec RECORD.
         */
        EXPORT GetAllKeyValues(BOOLEAN isUserSpecific = FALSE,
                               UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'FetchAll',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(GetAllKeyValuesResponseRec),
                    XPATH('FetchAllResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            RETURN UpdateForExceptions(soapResponse)[1];
        END;

        /**
         * Deletes the namespace defined for this module and all keys and values
         * defined within it.
         *
         * @param   isUserSpecific      If TRUE, the system will look only for
         *                              private keys; if FALSE then the system will
         *                              look for global keys; OPTIONAL, defaults
         *                              to FALSE
         * @param   timeoutInSeconds    The number of seconds to wait for the
         *                              underlying SOAPCALL to complete; set to zero
         *                              to wait forever; OPTIONAL, defaults to zero
         *
         * @return  A DeleteNamespaceResponseRec RECORD.
         */
        EXPORT DeleteNamespace(BOOLEAN isUserSpecific = FALSE,
                               UNSIGNED2 timeoutInSeconds = 0) := FUNCTION
            soapResponse := SOAPCALL
                (
                    MY_ESP_URL,
                    'DeleteNamespace',
                    {
                        STRING  pStoreName      {XPATH('StoreName')} := storeName;
                        STRING  pNamespace      {XPATH('Namespace')} := namespace;
                        BOOLEAN pUserSpecific   {XPATH('UserSpecific')} := isUserSpecific;
                    },
                    DATASET(DeleteNamespaceResponseRec),
                    XPATH('DeleteNamespaceResponse'),
                    HTTPHEADER('Authorization', ENCODED_CREDENTIALS),
                    TIMEOUT(timeoutInSeconds)
                );

            RETURN UpdateForExceptions(soapResponse)[1];
        END;

    END; // WithNamespace module

END; // Store module
