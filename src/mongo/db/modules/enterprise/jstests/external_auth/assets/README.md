## This document describes the steps to setting up the infrastructure for running the LDAP Authorization tests



#### Setting up an external LDAP server with the following properties

 - the domain name must be one of those specified in `baseLDAPUrls`
 - the following set of users/password and be added to the role corresponding to `defaultRole`
    - "ldapz\_ldap\_bind/Admin001"		with permission to do LDAP queries.
    - "ldapz\_admin/Secret123"			with reversible password
    - "ldapz\_mongodb/Admin001"		with the SPN set to `mongodb/localhost`
    - "ldapz\_kerberos1/Secret123"
    - "ldapz\_kerberos2/Secret123"		with userPrincipalName set to "ldapz_kerberos2@LDAPZ-TESTING.MONGODB.COM"
    - "ldapz\_ldap1/Secret123"
    - "ldapz\_ldap2/Secret123"			with uid set to "ldapz_ldap2"
    - "ldapz\_x509\_1/Secret123"
    - "ldapz\_x509\_2/Secret123"		with uid set to "ldapz\_x509\_2"
 - the full user DN must be the username followed by the `defaultUserDNSuffix`

#### Generating the TLS certificates with OpenSSL

- In openssl.conf, set `preserve = yes`
- ca.xxx are for the certificate authority
- gc.xxx are for the global controller (also the LDAP server)
- ldapz\_mongod.xxx are for the mongod server
- ldapz\_mongod\_cli\_x.xxx are for clients

##### For the certificate authority:

- generate the key file: `openssl genrsa -out ca.key 2048`
- generate the CA certificate: `openssl req -x509 -new -key ca.key -days 3650 -out ca.crt`
- generate the PEM file: `cat ca.key ca.crt > ca.pem` 
##### For the other certificates:

- replace `xxxx` with the username
- generate the xxxx.key file with `openssl genrsa -out xxxx.key 2048`
- using the above key, generate a certificate signing request `openssl req -new -key xxxx.key -out xxxx.csr`
- sign the CSR `openssl x509 -req -in xxxx.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out xxxx.crt -days 3650 -extfile xxxx_cli_v3.ext`. For the LDAP server, use  `-extfile xxxx_v3.ext` instead
- generate the PEM file `cat xxxx.key xxxx.crt > xxxx.pem`

The LDAP server and CA crt files then need to be installed onto the server itself, the process may be different for each platform. For Active Directory, this can be achieved with the "Certificates" Snap-In in mmc.exe and `certreq`. Details here: [Microsoft KB Article](https://support.microsoft.com/en-us/kb/321051)

#### Generating the Kerberos keytabs with MIT Kerberos

- ldapz\_kerberos1, ldapz\_kerberos2, ldapz\_mongodb and ldapz\_ldap\_bind
- point the KRB5_CONFIG environment variable to the the included krb5.conf file
- using ktutil, replace `xxxx` with the username
	- `addent -password -p xxxx -e arcfour-hmac -k 1`
	- enter the user's password
	- `wkt xxxx.keytab`
- the `-e` and `-k` flags may be set to a different value for different LDAP servers