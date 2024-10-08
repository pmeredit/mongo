/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

namespace mongo {
namespace audit {

/**
 * macOS will refuse to link this proxy without source files.
 * Windows will "succeed", but produce no output .lib.
 * Work around this by declaring a dummy TU with a symbol.
 */
int auditMongoDProxyAnchor() {
    return 42;
}
}  // namespace audit
}  // namespace mongo
