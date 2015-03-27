/*
* Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
*/

#include "mongo/platform/basic.h"

#include "rlp_token_iterator.h"

namespace mongo {
namespace fts {

    RlpTokenIterator::RlpTokenIterator(RlpEnvironment* rlpEnvironment)
        : _iterator(nullptr, RlpIteratorDeleteContext(rlpEnvironment)) {}

    RlpTokenIterator::RlpTokenIterator(RlpEnvironment* rlpEnvironment,
                                       BT_RLP_TokenIteratorC* iterator)
        : _iterator(iterator, RlpIteratorDeleteContext(rlpEnvironment)) {}

#if defined(_MSC_VER) && _MSC_VER < 1900
    RlpTokenIterator::RlpTokenIterator(RlpTokenIterator&& other)
        : _iterator(std::move(other._iterator)) {}

    RlpTokenIterator& RlpTokenIterator::operator=(RlpTokenIterator&& other) {
        _iterator = std::move(other._iterator);
        return *this;
    }
#endif

    void RlpTokenIterator::reset(RlpTokenIterator&& other) {
        _iterator = std::move(other._iterator);
    }

    bool RlpTokenIterator::next() {
        auto d = _iterator.get_deleter();
        return d.getRlpEnvironment()->BT_RLP_TokenIterator_Next(_iterator.get());
    }

    bool RlpTokenIterator::isStopWord() {
        auto d = _iterator.get_deleter();
        return d.getRlpEnvironment()->BT_RLP_TokenIterator_IsStopword(_iterator.get());
    }

    const BT_Char16* RlpTokenIterator::getToken() {
        auto d = _iterator.get_deleter();
        return d.getRlpEnvironment()->BT_RLP_TokenIterator_GetToken(_iterator.get());
    }

    const BT_Char16* RlpTokenIterator::getLemma() {
        auto d = _iterator.get_deleter();
        return d.getRlpEnvironment()->BT_RLP_TokenIterator_GetLemmaForm(_iterator.get());
    }

    const BT_Char16* RlpTokenIterator::getStem() {
        auto d = _iterator.get_deleter();
        return d.getRlpEnvironment()->BT_RLP_TokenIterator_GetStemForm(_iterator.get());
    }

    RlpTokenIterator::RlpIteratorDeleteContext::RlpIteratorDeleteContext(
        RlpEnvironment* rlpEnvironment)
        : _rlpEnvironment(rlpEnvironment) {}

    void RlpTokenIterator::RlpIteratorDeleteContext::operator()(
        BT_RLP_TokenIteratorC* iterator) const {
        if (iterator) {
            _rlpEnvironment->BT_RLP_TokenIterator_Destroy(iterator);
        }
    }

    RlpEnvironment* RlpTokenIterator::RlpIteratorDeleteContext::getRlpEnvironment() {
        return _rlpEnvironment;
    }

}  // namespace fts
}  // namespace mongo
