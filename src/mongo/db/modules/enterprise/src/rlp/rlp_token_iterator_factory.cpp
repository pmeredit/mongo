/*
* Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
*/
#include "mongo/platform/basic.h"

#include "rlp_token_iterator_factory.h"

namespace mongo {
namespace fts {

    RlpTokenIteratorFactory::RlpTokenIteratorFactory(RlpEnvironment* rlpEnvironment)
        : _factory(rlpEnvironment->BT_RLP_TokenIteratorFactory_Create(),
                   RlpIteratorFactoryDeleteContext(rlpEnvironment)) {}

    void RlpTokenIteratorFactory::setReturnReadings(bool flag) {
        auto d = _factory.get_deleter();
        d.getRlpEnvironment()->BT_RLP_TokenIteratorFactory_SetReturnReadings(_factory.get(), flag);
    }

    void RlpTokenIteratorFactory::setReturnCompoundComponents(bool flag) {
        auto d = _factory.get_deleter();
        d.getRlpEnvironment()->BT_RLP_TokenIteratorFactory_SetReturnCompoundComponents(
            _factory.get(), flag);
    }

    RlpTokenIterator RlpTokenIteratorFactory::createIterator(BT_RLP_ContextC* context) {
        auto d = _factory.get_deleter();

        BT_RLP_TokenIteratorC* iterator =
            d.getRlpEnvironment()->BT_RLP_TokenIteratorFactory_CreateIterator(_factory.get(),
                                                                              context);

        return RlpTokenIterator(d.getRlpEnvironment(), iterator);
    }

    RlpTokenIteratorFactory::RlpIteratorFactoryDeleteContext::RlpIteratorFactoryDeleteContext(
        RlpEnvironment* rlpEnvironment)
        : _rlpEnvironment(rlpEnvironment) {}

    void RlpTokenIteratorFactory::RlpIteratorFactoryDeleteContext::operator()(
        BT_RLP_TokenIteratorFactoryC* iterator) const {
        if (iterator) {
            _rlpEnvironment->BT_RLP_TokenIteratorFactory_Destroy(iterator);
        }
    }

    RlpEnvironment* RlpTokenIteratorFactory::RlpIteratorFactoryDeleteContext::getRlpEnvironment() {
        return _rlpEnvironment;
    }

}  // namespace fts
}  // namespace mongo
