# Migrating from Std.Crypto to Std.OpenSSL

This guide explains how to transition from the deprecated `Std.Crypto` module functions to their replacements in `Std.OpenSSL.*`.

## Overview of the Migration Process

The `Std.Crypto` module provided cryptographic functionality across four main areas:

- Algorithm enumeration (Supported* functions)
- Hashing operations
- Symmetric encryption
- Public key encryption (multiple variants differing by key source and passphrase type)

All cryptographic features are now unified under Std.OpenSSL.*, improving API consistency, simplifying the codebase, and enhancing OpenSSL integration.

## Namespace Migration Mapping

The following table shows how functional areas map from old to new namespaces:

| Functional Area | Old Namespace(s) | New Namespace |
|-----------------|------------------|---------------|
| Hash / Digest   | `Std.Crypto.Hashing()` | `Std.OpenSSL.Digest()` |
| Symmetric Ciphers | `Std.Crypto.SymmetricEncryption()`, `Std.Crypto.SymmEncryption()` | `Std.OpenSSL.Ciphers()` |
| Public Key (RSA Seal/Unseal, Sign/Verify) | Multiple: `PublicKeyEncryption*()`, `PKEncryption*()` (file, LFN, buffer variants) | `Std.OpenSSL.PublicKey`() |
| Algorithm Enumeration | `Std.Crypto.SupportedHashAlgorithms()`, etc. | `Std.OpenSSL.Digest.AvailableAlgorithms()`, `Std.OpenSSL.Ciphers.AvailableAlgorithms()` |
| Public Key Algorithm Enumeration | `Std.Crypto.SupportedPublicKeyAlgorithms()` (deprecated) | (No direct replacement yet) |

## Function Mapping Reference

The following table provides a direct mapping from deprecated functions to their replacements:

| Deprecated (Std.Crypto) | Replacement (Std.OpenSSL) |
|-------------------------|---------------------------|
| SupportedHashAlgorithms() | Digest.AvailableAlgorithms() |
| SupportedSymmetricCipherAlgorithms() | Ciphers.AvailableAlgorithms() |
| SupportedPublicKeyAlgorithms() | (No direct substitute) |
| Hashing.Hash() | Digest.Hash() |
| SymmetricEncryption.Encrypt() / Decrypt() | Ciphers.Encrypt() / Decrypt() |
| SymmEncryption.Encrypt() / Decrypt() | Ciphers.Encrypt() / Decrypt() |
| *PublicKey* Encrypt() variants | PublicKey.RSASeal() |
| *PublicKey* Decrypt() variants | PublicKey.RSAUnseal() |
| *PublicKey* Sign() variants | PublicKey.Sign() |
| *PublicKey* VerifySignature() variants | PublicKey.VerifySignature() |

**Note:** The "*PublicKey* variants" above include:

- PublicKeyEncryption() / PKEncryption()
- PublicKeyEncryptionFromLFN() / PKEncryptionFromLFN()
- PublicKeyEncryptionFromBuffer() / PKEncryptionFromBuffer()

These variants also include differences in passphrase types (VARSTRING vs DATA).

## Key Conceptual Changes

### 1. Key Source Abstraction Removal

**Previous approach:** Select a module based on whether your key is in a file path, LFN, or an in-memory buffer.

**New approach:**

- Public keys are always in PEM format
- Load the key content yourself (e.g., using `Std.File`, dataset read, or preferably obtain from a secrets library into a STRING
- Pass PEM content directly to `Std.OpenSSL.PublicKey` functions

### 2. Passphrase Type Normalization

**Previous approach:** Different APIs required `DATA` vs `VARSTRING` types.

**New approach:** Converge to the string type expected by the new API (usually STRING/VARSTRING). Convert `DATA` using safe encoding if it represented binary data, or treat it as raw bytes if the API permits.

### 3. Algorithm Specification

**Previous approach:** Implicit algorithm selection with limited control. Further, algorithms were chosen as part of the MODULE definition.

**New approach:** New digest/cipher functions expect an algorithm name parameter (e.g., 'SHA256', 'AES-256-CBC'). While some functions may have defaults, explicit specification is recommended for clarity and consistency.

## Public Key Material Handling

| Old Distinction | New Practice |
|-----------------|--------------|
| File path vs LFN vs Buffer modules | Keys are always in PEM format and used as STRINGs when calling new functions |

**Example (LFN to Buffer):**

```ecl
IMPORT Std;
publicKeyPEM  := GETSECRET('mykeys','mypublickeypem');
privateKeyPEM := GETSECRET('mykeys','myprivatekeypem');
sealed := Std.OpenSSL.PublicKey.RSASeal(rawData, publicKeyPEM);
```

## Algorithm Enumeration

**Before:**

```ecl
hashAlgos := Std.Crypto.SupportedHashAlgorithms();
```

**After:**

```ecl
hashAlgos := Std.OpenSSL.Digest.AvailableAlgorithms();
cipherAlgos := Std.OpenSSL.Ciphers.AvailableAlgorithms();
// For public key algorithms: consult documentation or maintain a static list (e.g., ['RSA']) until an API emerges.
```

## Performance Considerations

- **Hashing and symmetric operations:** Should have similar performance characteristics. For large streaming operations, consider manual chunking:

```ecl
// Pseudocode for chunk processing
chunks := DATASET( ... ); // each row contains chunked DATA
hashed := PROJECT(chunks, TRANSFORM(..., SELF.outHash := Std.OpenSSL.Digest.Hash(LEFT.chunk, 'SHA256')));
```

## Testing Strategy

1. **Golden Test Vectors:** Capture output from old implementation for representative inputs before migration
2. **Migrate code** following this guide
3. **Re-run tests** and verify outputs for:
   - Hash equivalence for chosen algorithms
   - Round-trip symmetric Encrypt/Decrypt operations
   - RSASeal/RSAUnseal round-trip functionality
   - Signature verification for both positive and negative test cases
4. **Compare outputs:** Acceptable differences may include base64 vs raw binary representation (normalize if needed)

## Common Migration Pitfalls

| Pitfall | Resolution |
|---------|------------|
| Missing algorithm parameter in new API call | Always pass explicit algorithm constant |
| Wrong passphrase type | Ensure STRING/VARSTRING; convert DATA appropriately |
| Embedded whitespace or newline differences in PEM keys | Trim or normalize; ensure correct PEM header/footer remain |
| Assuming multi-algorithm PublicKey support | Currently focus on RSA; extend later if library updates |

## Migration Checklist

- [ ] Enumerate all `Std.Crypto.*` references in codebase
- [ ] Classify each reference by category (hash, symmetric, public key)
- [ ] Introduce central constants for default digest and cipher algorithms
- [ ] Replace hashing calls and add explicit algorithm parameters
- [ ] Replace symmetric encryption and remove module instantiation pattern
- [ ] Refactor public key encryption: load key content, replace Encrypt/Decrypt with RSASeal/RSAUnseal, update Sign/VerifySignature calls
- [ ] Add comprehensive tests for each transformed operation
- [ ] Remove or quarantine legacy wrapper code

## Complete Migration Example

**Before:**

```ecl
IMPORT Std;
pkMod := Std.Crypto.PublicKeyEncryptionFromBuffer('RSA', pubKeyPEM, privKeyPEM, 'pw');
encPayload := pkMod.Encrypt(payload);
sig := pkMod.Sign(payload);
isOk := pkMod.VerifySignature(sig, payload);
hash := Std.Crypto.Hashing.Hash(payload);
sym := Std.Crypto.SymmetricEncryption('AES-256-CBC', 'symPass');
cipher := sym.Encrypt(payload);
plain := sym.Decrypt(cipher);
```

**After:**

```ecl
IMPORT Std;

EXPORT DEFAULT_DIGEST := 'SHA256';
EXPORT DEFAULT_CIPHER := 'AES-256-CBC';

// Public key operations
encPayload := Std.OpenSSL.PublicKey.RSASeal(payload, pubKeyPEM);
sig := Std.OpenSSL.PublicKey.Sign(payload, privKeyPEM, 'pw', DEFAULT_DIGEST);
isOk := Std.OpenSSL.PublicKey.VerifySignature(sig, payload, pubKeyPEM, DEFAULT_DIGEST);

// Hash operations
hash := Std.OpenSSL.Digest.Hash(payload, DEFAULT_DIGEST);

// Symmetric encryption
cipher := Std.OpenSSL.Ciphers.Encrypt(payload, DEFAULT_CIPHER, 'symPass');
plain := Std.OpenSSL.Ciphers.Decrypt(cipher, DEFAULT_CIPHER, 'symPass');
```

## Future-Proofing Recommendations

- **Centralize algorithm names** to ease future migrations
- **Encapsulate OpenSSL calls** in a small adapter module so future library evolution only affects one layer
- **Track HPCC Systems release notes** for introduction of public key algorithm enumeration functions
