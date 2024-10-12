# MONGODB-OIDC

An implementation of [OpenID Connect](https://openid.net/connect)
based authentication and authorization for MongoDB.
This authentication scheme is exposed via the SASL mechanism registry
using the name `MONGODB-OIDC`.

- [MONGODB-OIDC](#mongodb-oidc)
  - [Glossary](#glossary)
  - [Configuration](#configuration)
  - [IDPManager](#idpmanager)
  - [IdentityProvider](#identityprovider)
  - [AuthName](#authname)
  - [Authzn Claims](#authzn-claims)
    - [principalClaim](#principalclaim)
    - [authorizationClaim](#authorizationclaim)
  - [SASL exchange](#sasl-exchange)
    - [OIDCMechanismClientStep1](#oidcmechanismclientstep1)
      - [IDP Selection](#idp-selection)
    - [OIDCMechanismServerStep1](#oidcmechanismserverstep1)
    - [OIDCMechanismClientStep2](#oidcmechanismclientstep2)
  - [AuthzManagerExternalStateOIDC](#authzmanagerexternalstateoidc)

## Glossary

- **Identity Provider** (hereafter `IDP`): A third-party authentication service capable of issuing signed access tokens indicating a client's principal name and authorization grants.
- **Identity Token**: A type of [`JWT`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwt) issued by an `IDP` during authentication. This is generally intended for the authenticating client and may include `PII`.
- **Access Token**: A type of [`JWT`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwt) issued by an `IDP`, but unlike an Identity Token, is generally intended for third-party services (such as MongoDB) and does not include `PII`. Access Tokens also provide better mechanism for conveying application specific data such as an [`authorizationClaim`](#authorizationclaim).

## Configuration

OIDC is configured using a single, node-local server parameter: `oidcIdentityProviders`.
This parameter's value is expected to be an array of [`IDPConfiguration`](oidc_parameters.idl) objects.

To specify this value on startup, it must be encoded as a JSON string to be included in a configuration file or specified on the command line.
To specify this value at runtime, a normal BSON array of objects is permitted.

Note that when only one `IDP` is specified in a configuration, the `matchPattern` field may be omitted, as all users are expected to belong to the same `IDP`. When multiple `IDP`s are specified, this value MUST be present.

Multiple `IDPConfiguration`s may share the same `"issuer"` field value, but if they do, then each must have a unique `"audience"` field value.

## IDPManager

[`IDPManager`](idp_manager.h) processes the configuration presented by `oidcIdentityProviders` and spawns an instance of [`IdentityProvider`](#identityprovider) for each element in the top-level array. It also:

- Selects an [`IdentityProvider`](#identityprovider) to use for authentication during SASL exchange.
- Handles configuration reload during `{setParameter: 1, oidcIdentityProviders: [...]}`
- Creates an [`IDPJWKSRefresher`](idp_jwks_refresher.h) instance for each unique `"issuer"` in the configuration.
  The `IDPJWKSRefresher`, in turn, creates an owned [`JWKManager`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwkmanager)
  instance, responsible for fetching and maintaining the set of public keys (JWKS) obtained from the issuer.
  Ownership of the `IDPJWKSRefresher` is shared amongst all `IdentityProvider` instances for that issuer.
- Triggers each [`IDPJWKSRefresher`] to refresh their [`JWKManager`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwkmanager) based on configured polling intervals.

## IdentityProvider

[`IdentityProvider`](identity_provider.h) encapsulates the configuration for a single `IDP`, it also:

- Holds a shared pointer to a `IDPJWKSRefresher` instance, which manages the lifetime and refresh of a [`JWKManager`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwkmanager).
- Validates tokens present on behalf of clients.
- Translates principal and authorization claims into local [`AuthName`s](#authname).

## AuthName

All users authenticated using `MONGODB-OIDC` are authenticated against the `$external` database, and granted roles from the `admin` database.

To avoid conflicting with local and builtin `UserName` and `RoleName` definitions, all `IDP`s must be configured with an `authNamePrefix`. This prefix will be applied to `IDP` provided names in the format `{authNamePrefix}/{providedName}`.

For example, given an `IDP` configured with `authNamePrefix=myPrefix`, and the token:

```json
{
  "iss": "https://test.kernel.mongodb.com/oidc/issuer1",
  "sub": "user1@mongodb.com",
  "nbf": 1661374077,
  "exp": 2147483647,
  "aud": ["jwt@kernel.mongodb.com"],
  "nonce": "gdfhjj324ehj23k4",
  "mongodb-roles": ["myReadRole"]
}
```

A `User` will be synthesized with a `UserName` of `$external.myPrefix/user1@mongodb.com`, and a single `UserRole` of `admin.myPrefix/myReadRole`.

## Authzn Claims

### principalClaim

By default, user synthesis comes from the `sub` (Subject) claim of a
[`JWT`](https://github.com/mongodb/mongo/blob/master/src/mongo/crypto/README.JWT.md#jwt),
however the `IDP` configuration field `principalClaim` may specify
any other claim to pull this identity from.

### authorizationClaim

It is REQUIRED for all `IDP` configurations to specify a custom claim
to pull authorization grants from if `useAuthorizationClaim` is true (default).
This claim MUST be an array of string values which will be mapped into
`RoleName`s as specified in [`AuthName`](#authname) above.
If `useAuthorizationClaim` is false, then `authorizationClaim` may be
omitted from the configuration as the server will search for a user
document corresponding to the token's `principalClaim` to appropriately
authorize the user.

## SASL exchange

[`SaslOIDCServerMechanism`](sasl_oidc_server_conversation.cpp) implements
the SASL exchange used for `MONGODB-OIDC` based on the steps defined in
[`oidc_protocol.idl`](https://github.com/mongodb/mongo/blob/master/src/mongo/db/auth/oidc_protocol.idl).

All SASL payloads are `BinDataGeneral` encodings of BSON object octets
made up of the serialization of the steps defined below.

### OIDCMechanismClientStep1

```yaml
OIDCMechanismClientStep1:
  description: Client's opening request in saslStart or
    hello.speculativeAuthenticate
  strict: false
  fields:
    n:
      description: "Principal name of client"
      cpp_name: principalName
      type: string
      optional: true
```

A client begins authentication by sending this payload in a `saslStart` command.
The server responds by selecting an appropriate `IDP` from one of its configurations
and including various `endpoint` URIs and other `IDP` metadata in its response.

#### IDP Selection

If a `principalName` is available (e.g. from the client specified `mongodb://` URI),
it will be included as `n` here by the client as a hint to be used during
`IDP` selection. The server will check this value against `IdentityProvider` `matchPattern`s.
If no match is found, an error is returned.
If no hint is provided (and only one `IDP` was defined with no `matchPattern`) then
the one and only `IDP` selected by default.

Note that `IDP` selection will only occur among `IDPs` that have the `supportsHumanFlows` field
set to `true` (which is its default value). The server does not support `OIDCMechanism(Client|Server)Step1`
for `IDPs` with `supportsHumanFlows=false` because these are machine flow `IDPs` that are expected
to be able to acquire a token out-of-band without needing a `clientId` or other metadata from the
server. As a result, `clientId` is not a required field in `IDP` configurations that have
`supportsHumanFlows=false`.

### OIDCMechanismServerStep1

Once `IDP` selection completes, the server replies with the following message.
The client will use this information to contact the `IDP` and perform authentication.
Once it has acquired an `Access Token`, the client will proceed to step 2.

```yaml
    OIDCMechanismServerStep1:
        description: "Server's reply to clientStep1"
        strict: false
        fields
            issuer:
                description: >-
                    URL which describes the Authorization Server. This identifier should be
                    the iss of provided access tokens, and be viable for RFC8414
                    metadata discovery and RFC9207 identification.
                type: string
            clientId:
                description: "Unique client ID for this OIDC client"
                type: string
            requestScopes:
                description: "Additional scopes to request from IDP"
                type: array<string>
                optional: true
```

### OIDCMechanismClientStep2

A client presents its `AccessToken` to a MongoDB server to complete
authentication and authorization.

```yaml
OIDCMechanismClientStep2:
  description: "Client's request with signed token"
  strict: false
  fields:
    jwt:
      description: "Compact serialized JWT with signature"
      cpp_name: JWT
      type: string
```

Note that, if a client already has a valid `AccessToken`,
it may skip step 1 and present this token during `saslStart`
to authenticate in a single step. Indeed, this is the expected flow for
client applications authenticating as themselves rather than on behalf
of humans.

Once the SASL exchange has completed, the signed `Access Token`
is stowed in a synthesized `UserRequest` object as `mechanismData`
so that the user authorization grants may be (re)acquired via
[`AuthzManagerExternalStateOIDC`](#authzmanagerexternalstateoidc).

## AuthzManagerExternalStateOIDC

The [`AuthzManagerExtenalState` implementation for OIDC](authz_manager_external_state_oidc.cpp)
examines previously stowed `UserRequest::mechanismData` to revalidate
a previously presented token and extract its `authorizationClaim` for
synthesizing a set of `RoleName` grants according to the rules explained
in [`AuthName`](#authname) above.

This `AuthzManagerExternalState` is shimmed into the external state
setup process during startup so that it acts prior to any previously
enregistered provider
(e.g. `AuthzManagerExternalStateLDAP` on `mongod`
or `AuthzManagerExternalStateMongos` on `mongos`).
