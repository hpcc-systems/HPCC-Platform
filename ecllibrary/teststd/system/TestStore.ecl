/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.  All rights reserved.
############################################################################## */

IMPORT Std;

EXPORT TestStore := MODULE

    SHARED USER_NAME := '';
    SHARED USER_PW := '';
    SHARED ESP := '';

    SHARED STORE_NAME := 'hpcc_internal_selftest';
    SHARED STORE_NAMESPACE := 'ns_' + WORKUNIT;
    SHARED KEY_1 := 'some_key';
    SHARED VALUE_1 := 'fubar';

    SHARED kvStore := Std.System.Store(USER_NAME, USER_PW, ESP);
    SHARED namedKVStore := kvStore.WithNamespace(STORE_NAMESPACE, STORE_NAME);

    SHARED createStoreRes := kvStore.CreateStore(STORE_NAME);
    SHARED listStoresRes := kvStore.ListStores();
    SHARED listNamespacesRes := kvStore.ListNamespaces(STORE_NAME);
    SHARED setKeyValueRes := namedKVStore.SetKeyValue(KEY_1, VALUE_1);
    SHARED getKeyValueRes := namedKVStore.GetKeyValue(KEY_1);
    SHARED getAllKeyValuesRes := namedKVStore.GetAllKeyValues();
    SHARED getAllKeysRes := namedKVStore.GetAllKeys();
    SHARED deleteKeyValueRes := namedKVStore.DeleteKeyValue(KEY_1);
    SHARED deleteNamespaceRes := namedKVStore.DeleteNamespace();

    // Using SEQUENTIAL to reuse the above definitions in a stateful manner
    EXPORT Main := SEQUENTIAL
        (
            // Create a new store
            ASSERT((createStoreRes.succeeded OR createStoreRes.already_present) = TRUE);

            // Ensure our new store exists
            ASSERT(EXISTS(listStoresRes.stores(store_name = STORE_NAME)));

            // Set a new key/value
            ASSERT(setKeyValueRes.succeeded);

            // Ensure our namespace exists
            ASSERT(EXISTS(listNamespacesRes.namespaces(namespace = STORE_NAMESPACE)));

            // Fetch value for a key
            ASSERT(getKeyValueRes.was_found = TRUE);
            ASSERT(getKeyValueRes.value = VALUE_1);

            // Fetch all key/values in a namespace
            ASSERT(getAllKeysRes.namespace = STORE_NAMESPACE);
            ASSERT(EXISTS(getAllKeysRes.keys(key = KEY_1)));
            ASSERT(getAllKeyValuesRes.namespace = STORE_NAMESPACE);
            ASSERT(EXISTS(getAllKeyValuesRes.key_values(key = KEY_1 AND value = VALUE_1)));

            // Delete a key/value
            ASSERT(deleteKeyValueRes.succeeded);

            // Ensure our namespace still exists
            ASSERT(EXISTS(listNamespacesRes.namespaces(namespace = STORE_NAMESPACE)));

            // Ensure our deleted key/value is really deleted
            ASSERT(getKeyValueRes.was_found = FALSE);

            // Ensure all keys and values are deleted
            ASSERT(getAllKeyValuesRes.namespace = STORE_NAMESPACE);
            ASSERT(NOT EXISTS(getAllKeyValuesRes.key_values));

            // Delete the namespace
            ASSERT(deleteNamespaceRes.succeeded);

            // Ensure we see exceptions or empty values when querying empty namespace
            ASSERT(getAllKeyValuesRes.namespace = '');
            ASSERT(NOT EXISTS(getAllKeyValuesRes.key_values));
            ASSERT(getAllKeyValuesRes.has_exceptions);
            ASSERT(getKeyValueRes.was_found = FALSE);
            ASSERT(getKeyValueRes.value = '');

            ASSERT(TRUE);
        );

END;
