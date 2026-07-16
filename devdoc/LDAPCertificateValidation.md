# LDAP TLS Certificate Validation

This document describes how to enable TLS certificate validation for LDAPS connections in the
HPCC Platform. By default, the platform connects to LDAPS endpoints without validating the server
certificate. Enabling validation protects against Man-in-the-Middle (MITM) attacks on the
connection between HPCC and the LDAP directory server.

---

## Background

When the HPCC Platform connects to an LDAP server over LDAPS (port 636), TLS encrypts the
connection. However, encryption alone does not guarantee you are talking to the correct server.
Without certificate validation, an attacker positioned on the network path between HPCC and the
directory server can impersonate the server, intercept credentials, and inject fabricated
responses.

In enterprise deployments where Active Directory domain controllers span both cloud and
on-premises sites, LDAPS traffic can cross WAN links or cloud interconnects. Certificate
validation ensures the server on the other end is the legitimate directory server regardless of
which network segment the connection traverses.

---

## Configuration

Two attributes control TLS certificate validation in the LDAP configuration block:

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `ldapTLSValidation` | string | `disabled` | Validation mode: `disabled`, `permissive`, or `strict` |
| `ldapCACertFile` | string | _(empty)_ | Path to CA certificate file. Strongly recommended when `ldapTLSValidation` is `strict` or `permissive`. If omitted, the system trust store is used silently — no warning at startup. If the connection fails, the error message will suggest setting this. Requires the `hpcc` user to have read access to the system trust store (typically `/etc/ssl/certs/`) |

### Validation Modes

**`disabled`** *(default)*  
Certificate validation is not performed. The platform accepts any certificate presented by the
server. This preserves backward-compatible behaviour.

**`permissive`**  
Certificate validation is attempted. If validation cannot be enabled, an error is logged and the
connection proceeds. Use this mode when transitioning to full validation and you need to diagnose
issues without hard-failing.

**`strict`**  
Certificate validation is required. If validation cannot be enabled, the platform throws and will
not start. Use this mode in production.

### Example Configuration (environment.xml)

```xml
<ldap ldapTLSValidation="strict"
      ldapCACertFile="/etc/hpcc/certs/ldap-ca.crt"
      ... />
```

### Example Configuration (Helm values)

```yaml
ldap:
  ldapTLSValidation: "strict"
  ldapCACertFile: "/etc/hpcc/certs/ldap-ca.crt"
```

---

## Certificate Requirements

### LDAP Connection Pool

The HPCC Platform maintains a pool of persistent LDAP connections to support concurrent requests.
The pool may hold connections to multiple LDAP hosts (configured via `ldapAddress` as a
comma-separated list of replicas or domain controllers) that all share the same TLS configuration.

Because the CA certificate file (`ldapCACertFile`) is loaded into a per-connection TLS context
for every connection in the pool, **all servers in the pool must present certificates that chain to
a CA included in that file** (or in the system trust store if no file is specified).

This is not a practical constraint for well-formed deployments:

- **Active Directory**: All domain controllers in a domain receive certificates from the same
  internal enterprise CA. A single CA cert file covers every DC in the pool.
- **OpenLDAP replicas**: Replicas are typically provisioned from the same PKI, so the same CA
  cert applies to all.
- **Mixed CAs**: If pool members genuinely use certificates from different CAs, combine the
  required CA certificates into a single PEM bundle file and set `ldapCACertFile` to that bundle.

> **Note:** Each server in the pool may serve its own distinct leaf certificate — they do not all
> need to be identical. What matters is that every leaf certificate chains to a CA present in the
> configured file.

---

### Active Directory / Enterprise CA Pool Configuration

This section covers the correct way to configure TLS certificate validation for a pool of LDAP
servers backed by a proper enterprise Certificate Authority — Active Directory domain controllers
or OpenLDAP replicas in an organisation with an internal PKI.

#### How enterprise PKI handles pool certs automatically

With Active Directory, each domain controller is automatically issued a TLS certificate by the
domain's enterprise CA (typically Microsoft Active Directory Certificate Services, AD CS). These
certificates are provisioned without manual intervention and have the following properties that
make pool validation straightforward:

- The DC's fully-qualified domain name (e.g. `dc1.corp.example.com`) is included as a Subject
  Alternative Name (SAN) in its certificate
- All DC certificates are signed by the same enterprise CA root
- The CA certificate is distributed automatically to all domain-joined machines via Group Policy

The result: every server in the pool presents a valid, individually-identified certificate.
OpenLDAP verifies each connection independently — when HPCC connects to `dc1.corp.example.com`
the cert presented includes that hostname in its SAN, and when it connects to `dc2.corp.example.com`
that cert includes its own hostname. No special handling is needed beyond pointing HPCC at the
right CA cert.

For OpenLDAP replicas managed through an internal PKI the same applies: each replica is issued its
own cert from the internal CA, with its own hostname in the SAN.

#### Use hostnames, not IP addresses

In enterprise deployments always configure pool members by **hostname**, not IP address. This
matters for two reasons:

1. **Hostname verification**: Enterprise server certs list the server's DNS name as the SAN, not
   its IP. If HPCC connects by IP (`10.1.2.3`) but the cert only lists
   `dc1.corp.example.com`, validation fails even though it is the correct server.

2. **Stability**: Server IPs can change (VM migration, re-provisioning). Hostnames and their
   certs remain valid as long as DNS is updated.

In `environment.xml`, configure the `Hardware/Computer` entries with fully-qualified hostnames:

```xml
<Hardware>
  <Computer name="dc1" netAddress="dc1.corp.example.com"/>
  <Computer name="dc2" netAddress="dc2.corp.example.com"/>
  <Computer name="dc3" netAddress="dc3.corp.example.com"/>
</Hardware>

<LDAPServerProcess ...>
  <Instance computer="dc1" name="s1"/>
  <Instance computer="dc2" name="s2"/>
  <Instance computer="dc3" name="s3"/>
</LDAPServerProcess>
```

This produces `ldapAddress="dc1.corp.example.com|dc2.corp.example.com|dc3.corp.example.com"` in
the generated configuration, which matches the SAN values in each DC's certificate.

#### Obtaining the enterprise CA certificate

The enterprise CA root certificate is the only certificate HPCC needs — not the individual server
certs. There are several ways to obtain it:

**From a domain-joined Windows machine:**

```powershell
# List trusted root CAs and export the enterprise root
certutil -store Root
certutil -exportcert -f "<CA Common Name>" ca.crt
```

**From a Linux machine joined to the domain (via sssd/realmd):**

```bash
# The enterprise CA cert is typically already present in the system trust store
ls /etc/pki/ca-trust/source/anchors/   # RHEL/CentOS
ls /usr/local/share/ca-certificates/   # Debian/Ubuntu
```

**Via openssl (inspect what the server presents):**

```bash
# Connect to any DC and inspect its certificate chain
openssl s_client -connect dc1.corp.example.com:636 -showcerts </dev/null 2>&1
```
The last certificate in the chain output is the CA root. Copy the `-----BEGIN CERTIFICATE-----`
block to a file and use that as `ldapCACertFile`.

**Via your IT/PKI team**: Request the root CA certificate in PEM format. If the organisation uses
an intermediate CA, you may need a bundle containing both the intermediate and root.

#### Deploying the CA certificate to HPCC nodes

Copy the CA cert to a stable path on each HPCC node:

```bash
sudo cp enterprise-ca.crt /etc/hpcc/certs/ldap-ca.crt
```

For Kubernetes, mount it via a `Secret` or `ConfigMap`:

```yaml
ldap:
  ldapTLSValidation: "strict"
  ldapCACertFile: "/etc/hpcc/certs/ldap-ca.crt"
```

> **Note:** See the `ldapCACertFile` attribute description above for behavior when this is omitted.
> If the enterprise CA cert is already in the OS trust store on every node and the `hpcc` user
> has read access to `/etc/ssl/certs/`, `ldapCACertFile` may be omitted — but note that the
> platform's bundled OpenSSL may not reliably access the system trust store on all installations
> (see Troubleshooting).

#### Verifying each pool member before enabling strict mode

Before setting `ldapTLSValidation="strict"`, verify that every pool member passes validation
independently. HPCC probes all configured hosts at startup in strict and permissive modes, but
you can also check manually:

```bash
# Verify each DC in the pool
for dc in dc1.corp.example.com dc2.corp.example.com dc3.corp.example.com; do
    echo -n "$dc: "
    openssl s_client -connect "${dc}:636" -CAfile /etc/hpcc/certs/ldap-ca.crt </dev/null 2>&1 \
        | grep "Verify return"
done
```

Every host should show `Verify return code: 0 (ok)`. Any that do not will surface as errors
in HPCC's startup log when running in `permissive` mode, or cause startup failure in `strict`
mode. Resolve each before switching to `strict`.

#### Intermediate CAs and certificate bundles

Some enterprise PKIs use an intermediate CA between the root CA and the server certificates. In
this case `ldapCACertFile` must include the full chain from the intermediate up to the root,
concatenated in PEM format:

```bash
# Combine intermediate and root into a bundle
cat intermediate-ca.crt root-ca.crt > ldap-ca-bundle.crt
```

OpenLDAP will walk the chain from the server cert through the intermediate to the root when
validating. If only the root is provided but the server cert was issued by an intermediate,
validation will fail with a chain error.

---

## Verifying the Configuration

### Confirm the LDAP server is serving TLS

```bash
openssl s_client -connect <ldap-server>:636 -CAfile <ca-cert-file> </dev/null 2>&1 | grep -E "subject|issuer|Verify return"
```

A successful connection will show `Verify return code: 0 (ok)`, with `subject` showing your
server certificate CN and `issuer` showing your CA. If you see a certificate error, check that
the CN or SAN in the server certificate matches the hostname being connected to.

> **Note:** `openssl s_client` without `</dev/null` will hang waiting for stdin input. Always
> redirect stdin to avoid this.

### `ldapsearch` vs. HPCC TLS behaviour

On Ubuntu/Debian, the system `ldapsearch` tool is compiled against **GnuTLS**, while HPCC
ships its own vcpkg-bundled `libldap` compiled against **OpenSSL**. This means they use
different trust stores:

- **`ldapsearch`** (GnuTLS): trusts only certs in the system bundle
  (`/etc/ssl/certs/ca-certificates.crt`). The CA certificate must be added to the system store
  before `ldapsearch` will accept it.
- **HPCC** (OpenSSL): trusts whatever cert file is specified by `ldapCACertFile`. The CA
  does **not** need to be in the system trust store.

As a result, `ldapsearch` failing with a certificate error does not mean HPCC will fail, and
vice versa. To add a CA cert to the system store for `ldapsearch` verification:

```bash
sudo cp <ca-cert-file> /usr/local/share/ca-certificates/ldap-ca.crt
sudo update-ca-certificates
ldapsearch -H ldaps://<ldap-server>:636 -x -b "dc=example,dc=com" -s base
```

The authoritative test for HPCC is starting the platform with `ldapTLSValidation="strict"`
and verifying the startup log (see below).

### Confirm HPCC validates the certificate

Start the platform with `ldapTLSValidation="strict"` and a correct `ldapCACertFile`. If
validation fails (wrong CA cert, hostname mismatch, or expired cert), the platform will throw
during startup and log the cause.

Set `ldapTLSValidation="permissive"` first to diagnose issues without hard-failing, then switch
to `strict` once connectivity is confirmed.

---

## Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|-------------|------------|
| Platform fails to start with TLS error | `strict` mode and certificate validation cannot be enabled | Use `permissive` to diagnose; check platform logs for the specific error |
| `Verify return code: 20` from openssl | CA cert not trusted | Ensure `ldapCACertFile` points to the correct CA cert |
| `Verify return code: 62` from openssl | Hostname mismatch | Ensure server cert CN or SAN matches the `ldapAddress` value in config |
| Connection works in `permissive`, fails in `strict` | Validation failing silently in permissive | Check platform logs for error detail |
| Works locally, fails in Kubernetes | CA cert not mounted in pod | Verify `ldapCACertFile` path exists inside the container |
| Validation chain error despite correct CA cert | Intermediate CA missing from bundle | Concatenate intermediate and root CA certs into a single PEM bundle |
| Correct cert on server but hostname mismatch | Traffic routed through SSL-terminating load balancer | The cert presented belongs to the load balancer, not the LDAP server — run `openssl s_client -connect <ldapAddress>:636 -showcerts` to inspect what HPCC actually sees, then use that cert's CA |
| `ldapsearch` fails with cert error but HPCC works (or vice versa) | Different TLS backends — system `ldapsearch` uses GnuTLS; HPCC uses its bundled OpenSSL-linked `libldap` | They use separate trust stores. Add the CA to the system store (`update-ca-certificates`) for `ldapsearch` verification; HPCC only needs `ldapCACertFile` set correctly |
