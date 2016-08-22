/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

namespace mongo {
namespace queryable {

class AllocState;

void startQueryableEvictorThread(AllocState* allocState);

}  // namespace queryable
}  // namespace mongo
