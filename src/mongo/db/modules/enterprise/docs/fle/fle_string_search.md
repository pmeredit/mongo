# Queryable Encryption Substring/Suffix/Prefix Search

- [Parameters](#parameters)
- [Client Side Processing of Inserts](#client-side-processing-of-inserts)
  - [StrEncode](#strencode)
    - [StrEncode: Substring](#strencode-substring)
    - [StrEncode: Suffix and Prefix](#strencode-suffix-and-prefix)

The addition of the following query types in Queryable Encryption enables indexing an encrypted
field for string search using substring, suffix, or prefix queries:

- `substringPreview`
- `suffixPreview`
- `prefixPreview`

Fields designated with any of the above query types can only have values of BSON type `String`, and
must be valid UTF-8.

A field may be indexed for suffix and prefix queries by specifying both `suffixPreview` and `prefixPreview`
in that field's `QueryTypeConfig`. It is forbidden to specify `substringPreview` in addition to
`suffixPreview` or `prefixPreview`. It is also forbidden to specify any of the above query types in
addition to `equality` or `range`.

All of the above string search index types support case insensitive or diacritic insensitive queries.

All of the above index types come with automatic indexing for exact string lookups.

## Parameters

The following parameters are required for all three string search query types:

- `strMinQueryLength` - This dictates the shortest codepoint length that will be indexed. (`int32`, must be greater than 0)
- `strMaxQueryLength` - This dictates the longest codepoint length that will be indexed. (`int32`, must be greater than 0)
- `caseSensitive` - Enables case folding on the indexed substrings if set to `false`.
- `diacriticSensitive` - Enables diacritic folding on the indexed substrings if set to `false`.

The `substringPreview` query type requires one additional parameter:

- `strMaxLength` - The maximum codepoint length of the whole string allowed to be inserted and indexed for substring
  search. (`int32`, must be greater than 0)

## Client Side Processing of Inserts

When a client with auto-encryption support inserts a document containing a substring/suffix/prefix
indexed encrypted field, it is sent for query analysis, which replaces the plaintext value with a
`FLE2EncryptionPlaceholder` payload containing a `FLE2TextSearchInsertSpec` value. The
`FLE2TextSearchInsertSpec` indicates what type of string search indexing to use, and includes
the required parameters and the value to encrypt.

Afterwards, the client encryption library (`libmongocrypt`) converts the `FLE2EncryptionPlaceholder`
value into a `FLE2InsertUpdatePayloadV2` payload containing the ciphertext of the user value, and a
`TextSearchTokenSets` value. This `TextSearchTokenSets` contains the cryptographic tokens required
for indexing the user value for string search.

As with equality and range indexed types, indexing an encrypted string value for substring, suffix, or
prefix search requires the client encryption library to produce a list of derived values, which are
then used to derive the sets of cryptographic tokens in the `TextSearchTokenSets`.
Those cryptographic tokens are used in the server to create the `__safeContent__` tags used in lookups.

For substring search, this list of derived values consists of the unique UTF-8 substrings of the
user string value, subject to minimum and maximum length constraints, and case or diacritic
folding.

Suffix or prefix indexing only differs from substring in that the list of values only consists of
suffixes or prefixes of the user string, respectively.

To prevent leakage of information on the real length of the user string, some number of fake "padding"
values are appended to this list of derived values. How this number is calculated is
detailed in the [StrEncode](#StrEncode) section below.

The `TextSearchTokenSets` has the following format, in pseudocode:

```c
struct TextSearchTokenSets {
    ExactTokenSet exact_set;
    SubstringTokenSet substring_sets[substr_tag_count];
    SuffixTokenSet suffix_sets[suffix_tag_count];
    PrefixTokenSet prefix_sets[prefix_tag_count];
};
```

where `ExactTokenSet`, `SubstringTokenSet`, `SuffixTokenSet`, `PrefixTokenSet` are containers for
three cryptographic tokens (ESC, EDC, and Server), and the encrypted compaction token:

```c
struct ExactTokenSet {
    EDCTextExactDerivedFromDataTokenAndContentionFactorToken edc_token;
    ESCTextExactDerivedFromDataTokenAndContentionFactorToken esc_token;
    ServerTextExactDerivedFromDataToken server_token;
    int8_t encrypted_compaction_token[48];
};
struct SubstringTokenSet {
    EDCTextSubstringDerivedFromDataTokenAndContentionFactorToken edc_token;
    ESCTextSubstringDerivedFromDataTokenAndContentionFactorToken esc_token;
    ServerTextSubstringDerivedFromDataToken server_token;
    int8_t encrypted_compaction_token[48];
};
struct SuffixTokenSet {
    EDCTextSuffixDerivedFromDataTokenAndContentionFactorToken edc_token;
    ESCTextSuffixDerivedFromDataTokenAndContentionFactorToken esc_token;
    ServerTextSuffixDerivedFromDataToken server_token;
    int8_t encrypted_compaction_token[48];
};
struct PrefixTokenSet {
    EDCTextPrefixDerivedFromDataTokenAndContentionFactorToken edc_token;
    ESCTextPrefixDerivedFromDataTokenAndContentionFactorToken esc_token;
    ServerTextPrefixDerivedFromDataToken server_token;
    int8_t encrypted_compaction_token[48];
};
```

### StrEncode

The `StrEncode` function in `libmongocrypt` generates the derived values needed to build the
`TextSearchTokenSets` described above.

A user string, `s`, to be indexed for substring, suffix, or prefix, is passed through the `StrEncode`
algorithm which returns one transformed string for exact match indexing, `s_`, and three lists:
`T_substr`, `T_suffix`, `T_prefix` containing derived values for substring, suffix, or prefix
indexing, respectively. Any of these three lists may be empty if it's not applicable to the field's
query type config.

Depending on case and diacritic folding settings, the exact match indexed string `s_` may not be
exactly the same as the user value `s`, as it may have been case folded or stripped of diacritics. The
tokens & ciphertext comprising the `ExactTokenSet` piece of the `TextSearchTokenSets` are derived
from `s_`.

Each string in `T_substr` is used to derive one `SubstringTokenSet` added to the
`TextSearchTokenSets.substring_sets` array. The same process is applied to convert `T_suffix`
to `TextSearchTokenSets.suffix_sets`, and to convert `T_prefix` to `TextSearchTokenSets.prefix_sets`.

> **NOTE:** When deriving a cryptographic token from one of the strings generated by `StrEncode` (e.g. `s_`), the
> string value is first wrapped into a BSON value, before applying the PRF (HMAC) function. This means
> that the actual data buffer passed to PRF consists of the 4 byte length field, the string, and the
> 1 byte null terminator. This is done in order to be consistent with the way the user value is
> encrypted using AES-256-CBC, and with how tokens are derived for equality-indexed strings.

#### StrEncode: Substring

For substring-indexed values, `T_substr` lists all unique substrings of `s_` with codepoint lengths
between the min and max (inclusive) specified in `strMinQueryLength` and `strMaxQueryLength`,
respectively. If `s_` is greater than the specified `strMaxLength` (aka `mlen` in OST), then insertion
is forbidden, and the `StrEncode` function returns an error.

In addition, `T_substr` may also include one or more "padding" values, generated
to pad the number of token sets so that information about the length of `s` is not leaked.
The padding value is defined as the string `pad = s_ + '\xFF'`. In other words, it is the value `s_`
intentionally made to be an invalid UTF-8 sequence by appending the illegal byte `0xFF`. This is so
that the padding value itself cannot be queried for using encrypted string search.

The number of `pad` strings added to `T_substr` is determined solely on the byte length of the CBC-padded
ciphertext of `s`. When encrypting `s`, it is first wrapped as a BSON value, which adds 4 bytes of
length information and 1 null byte to the string. Then, the wrapped BSON value is encrypted with
AES-256-CBC, which has a 16-byte block size. Therefore, the final ciphertext will take
up `cbclen = 16 * ceil((strlen(s) + 5) / 16)` bytes. The only information about `s` that can be gleaned
from `cbclen` alone is that it is at most `cbclen - 5` bytes and at least `cbclen - 16 + 1` bytes long.
We want to make sure that the number of substrings we put in `T_substr` reveals only as much.

Since `padlen = cbclen - 5` is the longest string length that gives the same `cbclen` value as `strlen(s)`,
we use it to derive the correct size of `T_substr`, which we call `msize`.
Conceptually, `msize` is the upper bound on the number of derived values given the ciphertext length
and the query type parameters. By padding to `msize`, we ensure that any string with the same
ciphertext length results in the same number of derived values, meaning that we are not leaking
information about the string besides what is already known from the ciphertext length. In practice,
this is equal to the total number of substrings which would be generated for a string `S` of byte
length and codepoint length equal to `min(padlen, strMaxLength)`, and which have lengths
in the range `[strMinQueryLength..min(padlen, strMaxQueryLength)]`.

Finally, the number of `pad` values to add to `T_substr` is simply `msize` minus the number of
unique substrings of `s_`.

#### StrEncode: Suffix and Prefix

For suffix/prefix indexed values, `T_suffix` lists all the suffixes of `s_` with codepoint lengths
between the min and max lengths (inclusive) specified in `strMinQueryLength` and `strMaxQueryLength`,
respectively. Similarly, `T_prefix` lists all the bounded prefixes of `s_`.

As with `T_substr`, `T_suffix` and `T_prefix` may be padded with `pad` values, until the size of the
list reaches `msize`. For suffix and prefix, the `msize` calculation is much simpler, as it is just
`msize = min(strMaxQueryLength, padlen) - strMinQueryLength + 1`.
