/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/dbtests/dbtests.h"

namespace mongo {
bool isMongos() {
    return false;
}
}

namespace EnterprisePipelineTests {

using boost::intrusive_ptr;
using std::string;

using namespace mongo;
namespace Local {
class Base {
public:
    virtual string inputPipeJson() = 0;
    virtual string outputPipeJson() = 0;

    BSONObj pipelineFromJsonArray(const string& array) {
        return fromjson("{pipeline: " + array + "}");
    }
    virtual void run() {
        const BSONObj inputBson = pipelineFromJsonArray(inputPipeJson());
        const BSONObj outputPipeExpected = pipelineFromJsonArray(outputPipeJson());

        intrusive_ptr<ExpressionContext> ctx =
            new ExpressionContext(&_opCtx, NamespaceString("a.collection"));
        string errmsg;
        intrusive_ptr<Pipeline> outputPipe = Pipeline::parseCommand(errmsg, inputBson, ctx);
        ASSERT_EQUALS(errmsg, "");
        ASSERT(outputPipe != NULL);

        ASSERT_EQUALS(Value(outputPipe->writeExplainOps()), Value(outputPipeExpected["pipeline"]));
    }

    virtual ~Base() {}

private:
    OperationContextNoop _opCtx;
};

namespace coalesceLookUpAndUnwind {

class UnwindOnAs : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$same'}}"
               "]";
    }
    string outputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right', unwinding: {preserveNullAndEmptyArrays: false}}}]";
    }
};

class UnwindOnAsWithPreserveEmpty : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$same', preserveNullAndEmptyArrays: true}}"
               "]";
    }
    string outputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right', unwinding: {preserveNullAndEmptyArrays: true}}}]";
    }
};

class UnwindNotOnAs : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$from'}}"
               "]";
    }
    string outputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$from'}}"
               "]";
    }
};

}  // namespace coalesceLookUpAndUnwind
}  // namespace Local


namespace Sharded {
class Base {
public:
    // These all return json arrays of pipeline operators
    virtual string inputPipeJson() = 0;
    virtual string shardPipeJson() = 0;
    virtual string mergePipeJson() = 0;

    BSONObj pipelineFromJsonArray(const string& array) {
        return fromjson("{pipeline: " + array + "}");
    }
    virtual void run() {
        const BSONObj inputBson = pipelineFromJsonArray(inputPipeJson());
        const BSONObj shardPipeExpected = pipelineFromJsonArray(shardPipeJson());
        const BSONObj mergePipeExpected = pipelineFromJsonArray(mergePipeJson());

        intrusive_ptr<ExpressionContext> ctx =
            new ExpressionContext(&_opCtx, NamespaceString("a.collection"));
        string errmsg;
        mergePipe = Pipeline::parseCommand(errmsg, inputBson, ctx);
        ASSERT_EQUALS(errmsg, "");
        ASSERT(mergePipe != NULL);

        shardPipe = mergePipe->splitForSharded();
        ASSERT(shardPipe != NULL);

        ASSERT_EQUALS(Value(shardPipe->writeExplainOps()), Value(shardPipeExpected["pipeline"]));
        ASSERT_EQUALS(Value(mergePipe->writeExplainOps()), Value(mergePipeExpected["pipeline"]));
    }

    virtual ~Base() {}

protected:
    intrusive_ptr<Pipeline> mergePipe;
    intrusive_ptr<Pipeline> shardPipe;

private:
    OperationContextNoop _opCtx;
};

namespace coalesceLookUpAndUnwind {

class UnwindOnAs : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$same'}}"
               "]";
    }
    string shardPipeJson() {
        return "[]";
    }
    string mergePipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right', unwinding: {preserveNullAndEmptyArrays: false}}}]";
    }
};


class UnwindOnAsWithPreserveEmpty : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$same', preserveNullAndEmptyArrays: true}}"
               "]";
    }
    string shardPipeJson() {
        return "[]";
    }
    string mergePipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right', unwinding: {preserveNullAndEmptyArrays: true}}}]";
    }
};

class UnwindNotOnAs : public Base {
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$from'}}"
               "]";
    }
    string shardPipeJson() {
        return "[]";
    }
    string mergePipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}"
               ",{$unwind: {path: '$from'}}"
               "]";
    }
};

}  // namespace coalesceLookUpAndUnwind

namespace needsPrimaryShardMerger {
class needsPrimaryShardMergerBase : public Base {
public:
    void run() override {
        Base::run();
        ASSERT_EQUALS(mergePipe->needsPrimaryShardMerger(), needsPrimaryShardMerger());
        ASSERT(!shardPipe->needsPrimaryShardMerger());
    }
    virtual bool needsPrimaryShardMerger() = 0;
};

class LookUp : public needsPrimaryShardMergerBase {
    bool needsPrimaryShardMerger() {
        return true;
    }
    string inputPipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}]";
    }
    string shardPipeJson() {
        return "[]";
    }
    string mergePipeJson() {
        return "[{$lookUp: {from : 'coll2', as : 'same', localField: 'left', foreignField: "
               "'right'}}]";
    }
};

}  // namespace needsPrimaryShardMerger
}  // namespace Sharded

class All : public Suite {
public:
    All() : Suite("pipeline") {}
    void setupTests() {
        add<Local::coalesceLookUpAndUnwind::UnwindOnAs>();
        add<Local::coalesceLookUpAndUnwind::UnwindOnAsWithPreserveEmpty>();
        add<Local::coalesceLookUpAndUnwind::UnwindNotOnAs>();
        add<Sharded::coalesceLookUpAndUnwind::UnwindOnAs>();
        add<Sharded::coalesceLookUpAndUnwind::UnwindOnAsWithPreserveEmpty>();
        add<Sharded::coalesceLookUpAndUnwind::UnwindNotOnAs>();
        add<Sharded::needsPrimaryShardMerger::LookUp>();
    }
};

SuiteInstance<All> myall;

}  // namespace EnterprisePipelineTests
