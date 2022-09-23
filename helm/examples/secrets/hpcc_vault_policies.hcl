path "secret/data/ecl/*" {
    capabilities = ["read"]
}

path "secret/data/eclUser/*" {
    capabilities = ["read"]
}

path "secret/data/storage/*" {
    capabilities = ["read"]
}

path "secret/data/authn/*" {
    capabilities = ["read"]
}

path "sys/auth/approle" {
  capabilities = [ "create", "read", "update", "delete", "sudo" ]
}

# Configure the AppRole auth method
path "sys/auth/approle/*" {
  capabilities = [ "create", "read", "update", "delete" ]
}

# Create and manage roles
path "auth/approle/*" {
  capabilities = [ "create", "read", "update", "delete", "list" ]
}

# Write ACL policies
path "sys/policies/acl/*" {
  capabilities = [ "create", "read", "update", "delete", "list" ]
}

# Write test data
# Set the path to "secret/data/mysql/*" if you are running `kv-v2`
path "secret/mysql/*" {
  capabilities = [ "create", "read", "update", "delete", "list" ]
}