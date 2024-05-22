# Reading Source Documents

Source documents describing the mathematics of the cryptography used in our implementation
of Field Level Encryption often include glyphs and terminology not commonly used in C++
code authoring, or make generalizations which we have chosen to implement in specific,
more constrained fashions.

## Special notation

### Source document notation

| Symbol                                          | Meaning                                                                                                                                                                                                                                   |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| elem &#x2208; set                               | Indicates the left hand side is an element of the set specified on the right hand side. e.g. 42 &#x2208; { 0, 42, 65537 }                                                                                                                 |
| &#x22A5;                                        | "Bottom". In practice this translates to a value of `boost::none` and serializes to `0`.                                                                                                                                                  |
| F<sub>S</sub>[x<sub>1</sub>, x<sub>2</sub>,...] | Represents a chain of applications of the function `F`. See [below](#chain-application-of-functions)                                                                                                                                      |
| `SKE.Enc(key, plaintext)`                       | `SKE` represents a Symmetric Key Encryption scheme which may be used to `Enc`rypt the value in `plaintext` using an encryption key `key`. `AES-CBC` or `AES-CTR` in our uses.                                                             |
| `SKE.Dec(key, ciphertext)`                      | `Dec`rypt a payload previously encrypted using `SKE.Enc(...)`.                                                                                                                                                                            |
| `#expr`                                         | A simple hash mark prefixing another expression, typically one returning a string or list, indicates we are looking for the size of this list. (e.g. `#Edges` becomes `edges.size()`)                                                     |
| `[expr]`                                        | When a numeric expression is enclosed in a pair of square brackets, we interpret this as a set of contiguous natural numbers from `1` to `expr`. Note that this set is 1 based, unlike most ranges in computing which tend to be 0 based. |

#### Chain application of functions

When reading an expression in the form F<sub>S</sub>[x<sub>1</sub>, ..., x<sub>n</sub>], we apply the function `F` as many times
as there are elements in the brace enclosed list.

For the first iteration, we apply F(S, x<sub>1</sub>), the output of this function is propagated as the key to the next iteration
using x<sub>2</sub>, and so on. For example, in the expression given above, the calculation is performed as:

```c++
  Fs[x1, x2, x3] = F(F(F(S, x1), x2), x3);
```

### Code conventions

While mathematical documents commonly use super/subscript notations, our source files are ASCII constrained,
therefore we employ a simplified encoding scheme to preserve information otherwise lost to transcription.

| Symbol  | Translation   | Meaning                                                                                                              |
| ------- | ------------- | -------------------------------------------------------------------------------------------------------------------- |
| A `_` B | A<sub>B</sub> | Expressions involving two labels separated by an underscore indicate the suffix is a subscript of the leading label. |
| A `^` B | A<sup>B</sup> | Expressions involving two labels separated by a caret indicate the suffix is a superscript of the leading label.     |

Notes:

- Since F<sub>S</sub> appears so commonly, it is special cased to simply `Fs` rather than `F_S`.
- Symbols which use a combination of super and subscripts should list superscripts first, then subscripts.
- When super/subscripts contain multiple elements, curly braces (`{` and `}`) may be used to maintain grouping.

### Variables

| Symbol        | Meaning                                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| F             | The capital `F` refers to a PseudoRandom Function (Prf) used to perform a one-way transformation. See [Function](#function) below.    |
| S             | The capital `S` when not associated with a numeric subscript represents a "root" encryption key. See [Keys](#keys) below.             |
| K<sub>f</sub> | A capital `K` with a subscript of `f` indicates an encryption key unique to a specific key path `f`. See [Keys](#keys) below.         |
| f             | The lower case `f` used in formulae refers to a fully qualified path spec. e.g. `db.coll.field.nestedField`. See [Keys](#keys) below. |

#### Function

The term `F` in source documents refers to a generic function `F(key, value)` operating across a specific domain.
In FLE, we specify `F(key, value)` as [`FLEUtil::prf(key, value)`](https://github.com/mongodb/mongo/blob/r7.0.0/src/mongo/crypto/fle_crypto.cpp#L4882)
which is implemented as `HMAC_SHA256(key, value)`.

#### SKE

FLE v1 primarily uses `AES256-CBC`, while v2 adopts `AES256-CTR` for some, but not all payloads.
Carefully review all specs for places where the `CBC` variant must be used for backward compatibility.

#### Keys

In the MongoDB implementation of Field Level Encryption, we do not use a "root" key `S` at all.
We do generate unique per-fieldpath key K<sub>f</sub> however, and always treat expressions
of the form F<sub>S</sub>(f) (or `F(S, f)`) equivalent to simply K<sub>f</sub>.
This means that when evaluating expressions such as T<sub>esc</sub> := F<sub>S</sub>(f, 1, 2), we can regard it as T<sub>esc</sub> := F<sub>K<sub>f</sub></sub>(1, 2) or:
`T_{esc} := HMAC_SHA256(HMAC_SHA256(K_f, 1), 2);` see [examples](#token-example) for how this looks in practice.

#### Examples

##### Token Example

```c++
// T_esc := F_S[f, 1, 2]
auto ESCToken = FLEUtil::prf(FLEUtil::prf(K_f, 1), 2);
```

or, more accurately, to represent typed values:

```c++
// ESCToken := HMAC_SHA256(HMAC_SHA256(K_f, 1), 2);
auto ESCToken = FLECollectionTokenGenerator::generateESCToken(FLELevel1TokenGenerator::generateCollectionsLevel1Token(K_f));
```

A relationship between named/typed tokens can be found in [`fle_crypto_types.h`](https://github.com/mongodb/mongo/blob/e0a3dcd880c803657cc672b4c9155b2b6b503344/src/mongo/crypto/fle_crypto_types.h#L115)

##### Algorithm instruction Examples

###### Compaction insertion of anchor padding documents

for all i &#x2208; [#Edges]: db.esc.insert({\_id: F<sub>S<sub>1,d</sub></sub>(&#x22A5; || a + i), value: SKE.Enc(S<sub>2,d</sub>, 0 || 0)})

```c++
// Simplified psuedocode, assumes precalculated a, S_1d, and S_2d.
// Real implementation uses helper: ESCCollectionAnchorPadding::generatePaddingDocument
for (std::size_t i = 1; i <= edges.size(); ++i) {
    auto id = FLEUtil::prf(S_1d, std::tie(0ULL, a + i));
    auto value = FLEUtil::packAndEncrypt(std::make_tuple(0ULL, 0ULL), S_2d);
    client.insert(BSON("_id" << id, "value" << value));
}
```
