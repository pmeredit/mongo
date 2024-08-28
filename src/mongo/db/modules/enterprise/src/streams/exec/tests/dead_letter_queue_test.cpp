/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <memory>

#include <boost/date_time/date_parsing.hpp>
#include <boost/optional/optional.hpp>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/unittest/framework.h"
#include "mongo/util/time_support.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/in_memory_dead_letter_queue.h"
#include "streams/exec/tests/test_utils.h"
#include "streams/exec/util.h"


namespace streams {

using namespace mongo;

class DeadLetterQueueTest : public AggregationContextFixture {
public:
    DeadLetterQueueTest() : _context(std::get<0>(getTestContext(getServiceContext()))) {}

    BSONObjBuilder createDlqMsg() {
        Document doc{};
        return toDeadLetterQueueMsg(
            _context->streamMetaFieldName, doc, boost::make_optional<std::string>("foo bar"));
    }

protected:
    std::unique_ptr<Context> _context;
};

TEST_F(DeadLetterQueueTest, HasDLQTime) {
    auto dlq = dynamic_cast<InMemoryDeadLetterQueue*>(_context->dlq.get());
    dlq->addMessage(createDlqMsg());

    auto messages = dlq->getMessages();
    ASSERT(messages.size() == 1);

    const auto& msg = messages.front();
    auto dlqTimeElement = msg["dlqTime"];
    ASSERT(dlqTimeElement);

    auto dlqTime = dlqTimeElement.date();
    ASSERT(dlqTime.toMillisSinceEpoch() > 1000);  // ensure that the wall time isn't a 0-value.
}
}  // namespace streams
