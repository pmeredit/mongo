/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <functional>
#include <type_traits>

#include "mongo/base/clonable_ptr.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/crypto/fle_field_schema_gen.h"
#include "mongo/db/field_ref.h"
#include "mongo/db/matcher/schema/encrypt_schema_gen.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/util/errno_util.h"
#include "mongo/util/pcre.h"
#include "mongo/util/str.h"
#include "query_analysis.h"
#include "resolved_encryption_info.h"

using namespace mongo::query_analysis;

namespace mongo {

class EncryptionSchemaTreeNode;

/**
 * Explicitly declare a type for cloning an EncryptionSchemaTreeNode, for compatibility with
 * clonable_ptr and to avoid relying on the implicit clone factory which requires a fully defined
 * type. We need this since an EncryptionSchemaTreeNode holds a
 * clonable_ptr<EncryptionSchemaTreeNode> as a member of the class.
 */
template <>
struct clonable_traits<EncryptionSchemaTreeNode> {
    struct clone_factory_type {
        std::unique_ptr<EncryptionSchemaTreeNode> operator()(const EncryptionSchemaTreeNode&) const;
    };
};

/**
 * EncryptionSchemaMap requires that the NamespaceString used as the key does not contain a
 * TenantId. NamespaceStrings for an encryption schema map are parsed from the original BSON
 * command, from either "csfleEncryptionSchemas" or "csfleEncryptionInformation", both of which do
 * not contain TenantIds. Furthermore, TenantIds are applied by a proxy once a request is sent to
 * the server, so we don't expect a NamespaceString in a command object received by Query Analysis
 * to contain a TenantId either.
 */
using EncryptionSchemaMap = std::map<NamespaceString, std::unique_ptr<EncryptionSchemaTreeNode>>;

/**
 * A class that represents a node in an encryption schema tree.
 *
 * The children of this node are accessed via a string map, where the key used in the map represents
 * the next path component.
 *
 * An encryption schema tree can be constructed via two endpoints. One endpoint supports converting
 * BSON representing $jsonSchema (FLE 1), and the other supports converting BSON representing an
 * EncryptedFieldConfig (FLE 2).
 *
 * For FLE 1, example $jsonSchema with encrypted fields "user.ssn" and "account":
 *
 * {$jsonSchema: {
 *      type: "object",
 *      properties: {
 *          user: {
 *              type: "object",
 *              properties: {
 *                  ssn: {encrypt:{}},
 *                  address: {type: "string"}
 *              }
 *          },
 *          account: {encrypt: {}},
 *      }
 * }}
 *
 * Results in the following encryption schema tree:
 *
 *                   NotEncryptedNode
 *                       /    \
 *                 user /      \ account
 *                     /        \
 *        NotEncryptedNode    EncryptedNode
 *               /     \
 *          ssn /       \ address
 *             /         \
 *     EncryptedNode  NotEncryptedNode
 *
 * For FLE 2, example EncryptedFieldConfig with encrypted fields "user.ssn" and "account":
 *
 *  {
 *      ..., // encrypted collections information
 *      "fields":
 *          [{
 *              "keyId": ...,
 *              "path": "user.ssn",
 *              "bsonType": "string",
 *              "queries": {"queryType": "equality"}
 *          }, {
 *              "keyId": ...,
 *              "path": "account",
 *              "bsonType": "string",
 *              "queries": []
 *          }]
 *  }
 *
 * Results in the following encryption schema tree:
 *
 *                   NotEncryptedNode
 *                       /    \
 *                 user /      \ account
 *                     /        \
 *        NotEncryptedNode    EncryptedNode
 *               /           supportedQueries: []
 *          ssn /
 *             /
 *     EncryptedNode
 *    supportedQueries: [{"queryType": "equality"}]
 *
 * In this case, each EncryptedNode keeps track of the queries on which its path can be queried.
 */
class EncryptionSchemaTreeNode {
public:
    explicit EncryptionSchemaTreeNode(FleVersion parsedFrom) : parsedFrom(parsedFrom) {}
    struct PatternPropertiesChild {
        PatternPropertiesChild(StringData regexStringData,
                               std::unique_ptr<EncryptionSchemaTreeNode> child)
            : regex{std::string{regexStringData}}, child(std::move(child)) {
            uassert(51141,
                    str::stream() << "Invalid regular expression in 'patternProperties': "
                                  << regexStringData
                                  << " PCRE error string: " << errorMessage(regex.error()),
                    regex);
        }

        pcre::Regex regex;
        clonable_ptr<EncryptionSchemaTreeNode> child;

        bool operator==(const PatternPropertiesChild& other) const {
            return regex.pattern() == other.regex.pattern() && *child == *other.child;
        }

        bool operator!=(const PatternPropertiesChild& other) const {
            return !(*this == other);
        }

        bool operator<(const PatternPropertiesChild& other) const {
            return (regex.pattern() < other.regex.pattern());
        }
    };

    /**
     * Invokes either the JSON Schema or EncryptedFieldConfig parser depending on the values within
     * 'params'. This function is templated on the return type, either an EncryptionSchemaMap (i.e
     * Agg command), or a single std::unique_ptr<EncryptionSchemaTreeNode>. It is imperative that
     * QueryAnalysisParams that were parsed as multi schema are not parsed into a single schema
     * through this function.
     */
    template <typename T>
    static T parse(const QueryAnalysisParams& params) {
        MONGO_UNREACHABLE;
        return T();
    }
    /**
     * Converts a JSON schema, represented as BSON, into an encryption schema tree. Returns a
     * pointer to the root of the tree or throws an exception if either the schema is invalid or is
     * valid but illegal from an encryption analysis perspective.
     *
     * If 'schemaType' is kRemote, allows schema validation keywords which have no implication on
     * encryption since they are used for schema enforcement on mongod.
     */
    static std::unique_ptr<EncryptionSchemaTreeNode> parse(BSONObj schema,
                                                           EncryptionSchemaType schemaType);

    /**
     * Converts an EncryptedFieldConfig represented as BSON into an encryption schema tree. Returns
     * a pointer to the root of the tree or throws an exception if either the config is invalid or
     * is valid but illegal from an encryption analysis perspective.
     */
    static std::unique_ptr<EncryptionSchemaTreeNode> parseEncryptedFieldConfig(BSONObj efc);

    virtual ~EncryptionSchemaTreeNode() = default;

    virtual std::unique_ptr<EncryptionSchemaTreeNode> clone() const = 0;

    /**
     * Override this method to return the node's EncryptionMetadata, or boost::none if it holds
     * none.
     */
    virtual boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const = 0;

    /**
     * Returns true if this tree contains at least one EncryptionSchemaEncryptedNode or
     * EncryptionSchemaStateMixedNode.
     */
    virtual bool mayContainEncryptedNode() const;

    /**
     * Returns true if this tree contains at least one EncryptionSchemaEncryptedNode with the
     * FLE 1 random algorithm or FLE 2 algorithm or contains at least one
     * EncryptionSchemaStateMixedNode.
     */
    virtual bool mayContainRandomlyEncryptedNode() const;

    /**
     * Returns true if this tree contains at least one EncryptionSchemaEncryptedNode with the
     * FLE 2 range algorithm or contains at least one EncryptionSchemaStateMixedNode.
     */
    virtual bool mayContainRangeEncryptedNode() const;

    /**
     * Certain EncryptionSchemaTreeNode derived classes may contain literals, stored in-place to
     * mark for encryption. This function returns a vector for holding them or boost::none if the
     * derived class type does not support attached literals.
     *
     * This method does not return literals from any of this node's descendants in the tree.
     */
    virtual boost::optional<std::vector<std::reference_wrapper<ExpressionConstant>>&> literals() {
        return boost::none;
    }

    /**
     * Adds 'node' at 'path' under this node. Adds unencrypted nodes as neccessary to
     * reach the final component of 'path'. Returns a pointer to a node that was overwritten or
     * nullptr if there did not already exist a node with the given path. It is invalid to call
     * this function with an empty FieldRef.
     */
    clonable_ptr<EncryptionSchemaTreeNode> addChild(FieldRef path,
                                                    std::unique_ptr<EncryptionSchemaTreeNode> node);

    /**
     * Adds 'node' as a special "wildcard" child which is used for all field names that don't have
     * explicit child nodes. For use when parsing $jsonSchema. For instance, consider the schema
     *
     * {
     *   type: "object",
     *   properties: {a: {type: "number"}, b: {type: "string"}},
     *   required: ["a", "b"],
     *   additionalProperties: {encrypt: {}}
     * }
     *
     * This schema matches objects where "a" is number, "b" is a string, and all other properties
     * are encrypted. This requires a special child in the encryption tree which has no particular
     * field name associated with it:
     *
     *                   NotEncryptedNode
     *                  /    |           \
     *               a /     | b          \ *
     *                /      |             \
     *  NotEncryptedNode  NotEncryptedNode  EncryptedNode
     *
     * The "*" in the diagram above indicates wildcard behavior: this child applies for all field
     * names other than "a" and "b".
     */
    void addAdditionalPropertiesChild(std::unique_ptr<EncryptionSchemaTreeNode> node) {
        tassert(6329202,
                "Additional properties only acceptable when parsed from JSONSchema with FLE 1.",
                this->parsedFrom == FleVersion::kFle1);
        tassert(6329204,
                "New children must have the same FLE version as their parent.",
                this->parsedFrom == node->parsedFrom);

        _additionalPropertiesChild = std::move(node);
    }

    /**
     * Adds 'node' as a special child associated with a regular expression rather than a fixed field
     * name. For use when parsing $jsonSchema. For instance, consider the schema
     *
     * {
     *   type: "object",
     *   properties: {a: {type: "number"}, b: {type: "string"}},
     *   patternProperties: {"^c": {encrypt: {}}}
     * }
     *
     * This schema matches objects where "a" is a number (if it exists), "b" is a string (if it
     * exists), and any property names which begin with "c" are encrypted. The 'patternProperties'
     * keyword results in a node in the encryption tree which is associated with the regex /^c/. The
     * encryption schema tree would look like this:
     *
     *                   NotEncryptedNode
     *                  /    |           \
     *               a /     | b          \ /^c/
     *                /      |             \
     *  NotEncryptedNode  NotEncryptedNode  EncryptedNode
     */
    void addPatternPropertiesChild(StringData regex,
                                   std::unique_ptr<EncryptionSchemaTreeNode> node) {
        tassert(6329205,
                "Pattern properties only acceptable when parsed from JSONSchema with FLE 1.",
                this->parsedFrom == FleVersion::kFle1);
        tassert(6329206,
                "New children must have the same FLE version as their parent.",
                this->parsedFrom == node->parsedFrom);

        _patternPropertiesChildren.insert(PatternPropertiesChild{regex, std::move(node)});
    }

    /**
     * If the given path maps to an encryption node in the tree then returns the associated
     * EncryptionMetadata, otherwise returns boost::none. Any numerical path components will
     * *always* be treated as field names, not array indexes.
     */
    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadataForPath(
        const FieldRef& path) const {
        auto node = getNode(path);
        return getEncryptionMetadataForNode(node);
    }

    /**
     * Returns true if the prefix passed in is the prefix of an encrypted path. Returns false if
     * the prefix does not exist. Should not be called if any part of the prefix is encrypted.
     */
    bool mayContainEncryptedNodeBelowPrefix(const FieldRef& prefix) const {
        return _mayContainEncryptedNodeBelowPrefix(prefix, 0);
    }

    /**
     * Returns true if the prefix passed in is the prefix of arange encrypted path .Returns false
     * if the prefix does not exist. Should not be called if any part of the prefix is encrypted.
     */
    bool mayContainRangeEncryptedNodeBelowPrefix(const FieldRef& prefix) const {
        return _mayContainRangeEncryptedNodeBelowPrefix(prefix, 0);
    }

    /**
     * Returns the node at a given path if it exists. Returns nullptr if no such path exists in the
     * tree. Respects additional and pattern properties. Throws an exception if there are multiple
     * matching nodes with conflicting metadata.
     */
    const EncryptionSchemaTreeNode* getNode(FieldRef path) const {
        return _getNode(path, 0);
    }
    EncryptionSchemaTreeNode* getNode(FieldRef path) {
        return const_cast<std::remove_const_t<decltype(this)>>(
            const_cast<std::add_const_t<decltype(this)>>(this)->_getNode(path, 0));
    }

    /**
     * Remove the specified node from the schema. Does nothing if path does not exist. Returns true
     * if a node was removed. Ignores additional and pattern properties.
     */
    bool removeNode(FieldRef path);

    // Note that comparing EncryptionSchemaStateMixedNodes for equality will fail, since their
    // encryption metadata isn't know until a query is run.
    bool operator==(const EncryptionSchemaTreeNode& other) const;

    bool operator!=(const EncryptionSchemaTreeNode& other) const {
        return !(*this == other);
    }

    /**
     * Returns whether this tree and 'other' are equivalent with respect to FLE 2 encryption. It
     * considers equality of encrypted leaves only. That is, it ignores children specified by regex
     * or 'additionalProperties', since none should exist for a tree built from
     * 'encryptionInformation'. As with regular equality, comparing EncryptionSchemaStateMixedNodes
     * for encrypted leaf equality will fail.
     */
    bool isFle2LeafEquivalent(const EncryptionSchemaTreeNode& other) const;

    /**
     * markEncryptedObjectArrayElements visits this node and its children, marking any encrypted
     * nodes within the tree to indicate that they reside within an encrypted object array, making
     * them unusable in the pipeline. The traversal down the tree stops early when we reach an
     * EncryptionSchemaEncryptedObjectArrayNode (i.e nested array). This is because each
     * EncryptionSchemaEncryptedObjectArrayNode is responsible for its own subtree, so an outer
     * encrypted array can't mark nodes that belong to another encrypted array.
     */
    virtual void markEncryptedObjectArrayElements();

    /**
     * unwindEncryptedObjectArrayElements visits this node and its children, clearing the flag of
     * any encrypted nodes to indicate they no longer reside within an encrypted object array. This
     * makes the encrypted nodes usable in the pipeline. The traversal down the tree stops early
     * when we reach an EncryptionSchemaEncryptedObjectArrayNode (i.e nested array). This is because
     * each EncryptionSchemaEncryptedObjectArrayNode is responsible for its own subtree, so an outer
     * encrypted array can't unwind the contents of a nested encrypted array.
     */
    virtual void unwindEncryptedObjectArrayElements();

    const FleVersion parsedFrom;

private:
    static boost::optional<ResolvedEncryptionInfo> getEncryptionMetadataForNode(
        const EncryptionSchemaTreeNode* node) {
        if (node) {
            return node->getEncryptionMetadata();
        }
        return boost::none;
    }

    /**
     * Returns a const pointer to the child if it exists. Ignores additionalProperties and
     * patternProperties children.
     */
    const EncryptionSchemaTreeNode* getNamedChild(StringData name) const {
        auto childrenIt = _propertiesChildren.find(name);
        if (childrenIt != _propertiesChildren.end()) {
            return childrenIt->second.get();
        }
        return nullptr;
    }

    EncryptionSchemaTreeNode* getNamedChild(StringData name) {
        auto childrenIt = _propertiesChildren.find(name);
        if (childrenIt != _propertiesChildren.end()) {
            return childrenIt->second.get();
        }
        return nullptr;
    }

    /**
     * Returns a list of child nodes under this node with name 'name'.
     *
     * For a tree constructed from $jsonSchema, only considers the subschemas that are
     * relevant. This follows the rules associated with the JSON Schema 'properties',
     * 'patternProperties', and 'additionalProperties' keywords. If there is a child added to the
     * tree via addChild() with the edge name exactly matching 'name', then that child will be
     * included in the output list. In addition, children added via addPatternPropertiesChild()
     * whose regex matches 'name' will be included in the output list. If no regular addChild()
     * nodes or 'patternProperties' child nodes are found, but a node has been added via
     * addAdditionalPropertiesChild(), then returns this 'additionalProperties' child. If no child
     * with 'name' exists, no 'patternProperties' child whose regex matches 'name' existis, and
     * there is no 'additionalProperties' child, then returns an empty vector.
     *
     * For a tree constructed from EncryptedFieldConfig, only a child added via addChild() with an
     * edge name matching 'name' will be included.
     */
    std::vector<EncryptionSchemaTreeNode*> getChildrenForPathComponent(StringData name) const;

    /**
     * This method is responsible for recursively descending the encryption tree until the end of
     * the path is reached or there's no edge to take. The 'index' parameter is used to indicate
     * which part of 'path' we're currently at, and is expected to increment as we descend the tree.
     *
     * Throws an AssertionException if 'path' contains a prefix to an encrypted field.
     *
     * Throws if multiple relevant subschemas return conflicting encryption metadata. This can
     * happen for 'patternProperties', since we may need to descend the subtrees for multiple
     * matching patterns.
     */
    const EncryptionSchemaTreeNode* _getNode(const FieldRef& path, size_t index = 0) const;

    bool _mayContainEncryptedNodeBelowPrefix(const FieldRef& prefix, size_t level) const;

    bool _mayContainRangeEncryptedNodeBelowPrefix(const FieldRef& prefix, size_t level) const;

    StringMap<clonable_ptr<EncryptionSchemaTreeNode>> _propertiesChildren;

    // Holds any children which are associated with a regex rather than a specific field name. This
    // should be empty when parsing EncryptedFieldConfig.
    std::set<PatternPropertiesChild> _patternPropertiesChildren;

    // If non-null, this special child is used when no applicable child is found by name in
    // '_propertiesChildren' or by regex in '_patternPropertiesChildren'. Used to implement
    // encryption analysis for the 'additionalProperties' keyword. This should be null when parsing
    // EncryptedFieldConfig.
    clonable_ptr<EncryptionSchemaTreeNode> _additionalPropertiesChild;
};

/**
 * Represents a path that is not encrypted. When parsing $jsonSchema, this may appear as an internal
 * node or a leaf node. When parsing EncryptedFieldConfig, this may only appear as an internal node.
 */
class EncryptionSchemaNotEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    EncryptionSchemaNotEncryptedNode(FleVersion parsedFrom)
        : EncryptionSchemaTreeNode(parsedFrom) {}

    EncryptionSchemaNotEncryptedNode(EncryptionSchemaTreeNode&& other)
        : EncryptionSchemaTreeNode(std::move(other)) {}

    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const final {
        return boost::none;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> clone() const final {
        return std::make_unique<EncryptionSchemaNotEncryptedNode>(*this);
    }
};

static inline void assertRuntimeEncryptedMetadata() {
    uasserted(31133,
              "Cannot get metadata for path whose encryption properties are not known until "
              "runtime.");
}
/**
 * Node which represents an encrypted field per the corresponding JSON Schema, or per the
 * EncryptedFieldConfig. A path is considered encrypted only if it's final component lands on this
 * node. EncryptedNodes may be found within an EncryptionSchemaEncryptedObjectArrayNode, in which
 * case they are marked with the _isWithinEncryptedArray flag set to true. In this case,
 * EncryptedNodes behave like an EncryptionSchemaStateMixedNode.
 */
class EncryptionSchemaEncryptedNode final : public EncryptionSchemaTreeNode {
public:
    EncryptionSchemaEncryptedNode(ResolvedEncryptionInfo metadata, FleVersion parsedFrom)
        : EncryptionSchemaTreeNode(parsedFrom), _metadata(std::move(metadata)) {}

    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const final {
        // As long as we are within an encrypted array, we can't reference this node.
        if (_isWithinEncryptedArray) {
            assertRuntimeEncryptedMetadata();
        }
        return _metadata;
    }

    bool mayContainEncryptedNode() const final {
        return true;
    }

    // If we have are within an encrypted object array, we try to mimic a mixed node.
    bool mayContainRandomlyEncryptedNode() const final {
        return _isWithinEncryptedArray
            ? true
            : (_metadata.algorithmIs(FleAlgorithmEnum::kRandom) || _metadata.isFle2Encrypted());
    }

    bool mayContainRangeEncryptedNode() const final {
        return _isWithinEncryptedArray
            ? true
            : (_metadata.isFle2Encrypted() && _metadata.algorithmIs(Fle2AlgorithmInt::kRange));
    }

    std::unique_ptr<EncryptionSchemaTreeNode> clone() const final {
        return std::make_unique<EncryptionSchemaEncryptedNode>(*this);
    }

    boost::optional<std::vector<std::reference_wrapper<ExpressionConstant>>&> literals() final {
        return _literals;
    }

    void markEncryptedObjectArrayElements() override;

    void unwindEncryptedObjectArrayElements() override;

private:
    const ResolvedEncryptionInfo _metadata;

    std::vector<std::reference_wrapper<ExpressionConstant>> _literals;
    bool _isWithinEncryptedArray{false};
};

/**
 * Node which represents a uniform encrypted array, where the schema of each element of the array is
 * guaranteed to be the identical. This is required to represent the output array of a $lookup stage
 * on an encrypted foreign collection. EncryptionSchemaEncryptedObjectArrayNode closely mimics a
 * EncryptionSchemaStateMixedNode.
 *
 * When we construct an EncryptionSchemaEncryptedObjectArrayNode, we
 * obtain the children of an existing node, and mark all the encrypted children as being within an
 * encrypted object array, making them un-usable without an unwind of the array. An
 * EncryptionSchemaEncryptedObjectArrayNode does not permit either marking or unwinding its children
 * by any other encrypted object array. This prevents an outer encrypted array from incorrectly
 * marking or unwinding nodes within an internal encrypted array for which it is not responsible.
 */
class EncryptionSchemaEncryptedObjectArrayNode final : public EncryptionSchemaTreeNode {
public:
    // Move internals out of source encryption schema tree.
    EncryptionSchemaEncryptedObjectArrayNode(EncryptionSchemaTreeNode&& other)
        : EncryptionSchemaTreeNode(std::move(other)) {
        tassert(9687204,
                "Invalid encrypted array creation source",
                EncryptionSchemaTreeNode::mayContainEncryptedNode());
        // Mark children to indicate they within an encrypted lookup array.
        EncryptionSchemaTreeNode::markEncryptedObjectArrayElements();
    }

    // Mimics EncryptionSchemaStateMixedNode behavior, and throwns an exception if call
    // getEncryptionMetadata() on it. This prevents the encrypted array from being used in
    // operations within a pipeline.
    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const final {
        assertRuntimeEncryptedMetadata();
        return boost::none;
    }

    // Mimics EncryptionSchemaStateMixedNode behavior.
    bool mayContainRandomlyEncryptedNode() const final {
        return true;
    }

    // Mimics EncryptionSchemaStateMixedNode behavior.
    bool mayContainRangeEncryptedNode() const final {
        return true;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> clone() const final {
        return std::make_unique<EncryptionSchemaEncryptedObjectArrayNode>(*this);
    }

    void markEncryptedObjectArrayElements() override;

    void unwindEncryptedObjectArrayElements() override;

    std::unique_ptr<EncryptionSchemaTreeNode> unwind();
};

/**
 * Node which represents a field which may or may not be encrypted. Since the actual state of the
 * node can't be known before the query is actually executed, attempting to get the encryption
 * metadata of this node will throw an exception.
 */
class EncryptionSchemaStateMixedNode final : public EncryptionSchemaTreeNode {
public:
    EncryptionSchemaStateMixedNode(FleVersion parsedFrom) : EncryptionSchemaTreeNode(parsedFrom) {}

    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const final {
        assertRuntimeEncryptedMetadata();
        return boost::none;
    }

    bool mayContainEncryptedNode() const final {
        // The field may be encrypted at runtime, safest option is to return true.
        return true;
    }

    bool mayContainRandomlyEncryptedNode() const final {
        return true;
    }

    bool mayContainRangeEncryptedNode() const final {
        return true;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> clone() const final {
        return std::make_unique<EncryptionSchemaStateMixedNode>(*this);
    }
};

/**
 * Node which represents a field for which we can choose an encryption status and type at a later
 * time. When this node exists in a schema tree, it indicates a referenceable path which does not
 * yet have an ResolvedEncryptionInfo assigned but could support one.
 *
 * This node has two possible futures. Either an encryption info will be chosen for it when it is
 * compared to an encrypted node of that type and it will be converted into an
 * EncryptionSchemaEncryptedNode. At that time, the attached literals would be marked for
 * encryption.
 *
 * Alternatively, it may be compared to something unencrypted or be staged for evaluation by the
 * server. If this is the case, it must become an EncryptionSchemaNotEncryptedNode and any attached
 * literals should be left alone and forgotten. If a schema tree reaches its final state and one of
 * these nodes still exists, the effect is the same as manually converting it to an
 * EncryptionSchemaNotEncryptedNode.
 */
class EncryptionSchemaUnknownNode final : public EncryptionSchemaTreeNode {
public:
    boost::optional<ResolvedEncryptionInfo> getEncryptionMetadata() const final {
        return boost::none;
    }

    std::unique_ptr<EncryptionSchemaTreeNode> clone() const final {
        return std::make_unique<EncryptionSchemaUnknownNode>(*this);
    }

    boost::optional<std::vector<std::reference_wrapper<ExpressionConstant>>&> literals() final {
        return _literals;
    }

private:
    std::vector<std::reference_wrapper<ExpressionConstant>> _literals;
};

/**
 * Invokes either the JSON Schema or EncryptedFieldConfig parser depending on the values within
 * 'params'.
 */
template <>
EncryptionSchemaMap EncryptionSchemaTreeNode::parse<EncryptionSchemaMap>(
    const QueryAnalysisParams& params);

/**
 * Invokes either the JSON Schema or EncryptedFieldConfig parser depending on the values within
 * 'params'. It is imperative that QueryAnalysisParams that were parsed as multi schema are not
 * parsed into a single schema through this function.
 */
template <>
std::unique_ptr<EncryptionSchemaTreeNode>
EncryptionSchemaTreeNode::parse<std::unique_ptr<EncryptionSchemaTreeNode>>(
    const QueryAnalysisParams& params);

}  // namespace mongo
