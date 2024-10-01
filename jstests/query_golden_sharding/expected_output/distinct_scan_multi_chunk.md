## 1. distinct on shard key
### Distinct on "shardKey", with filter: { }
### Expected results
`[ "chunk1_s0_0", "chunk1_s0_1", "chunk1_s0_2", "chunk1_s1_0", "chunk1_s1_1", "chunk1_s1_2", "chunk2_s0_0", "chunk2_s0_1", "chunk2_s0_2", "chunk2_s1_0", "chunk2_s1_1", "chunk2_s1_2", "chunk3_s0_0", "chunk3_s0_1", "chunk3_s0_2", "chunk3_s1_0", "chunk3_s1_1", "chunk3_s1_2" ]`
### Distinct results
`[ "chunk1_s0_0", "chunk1_s0_1", "chunk1_s0_2", "chunk1_s1_0", "chunk1_s1_1", "chunk1_s1_2", "chunk2_s0_0", "chunk2_s0_1", "chunk2_s0_2", "chunk2_s1_0", "chunk2_s1_1", "chunk2_s1_2", "chunk3_s0_0", "chunk3_s0_1", "chunk3_s0_2", "chunk3_s1_0", "chunk3_s1_1", "chunk3_s1_2" ]`
### Summarized explain
```json
{
	"mergerPart" : [
		{
			"stage" : "SHARD_MERGE"
		}
	],
	"shardsPart" : {
		"distinct_scan_multi_chunk-rs0" : {
			"rejectedPlans" : [ ],
			"winningPlan" : [
				{
					"stage" : "PROJECTION_COVERED",
					"transformBy" : {
						"_id" : 0,
						"shardKey" : 1
					}
				},
				{
					"direction" : "forward",
					"indexBounds" : {
						"shardKey" : [
							"[MinKey, MaxKey]"
						]
					},
					"indexName" : "shardKey_1",
					"isFetching" : false,
					"isMultiKey" : false,
					"isPartial" : false,
					"isShardFiltering" : true,
					"isSparse" : false,
					"isUnique" : false,
					"keyPattern" : {
						"shardKey" : 1
					},
					"multiKeyPaths" : {
						"shardKey" : [ ]
					},
					"stage" : "DISTINCT_SCAN"
				}
			]
		},
		"distinct_scan_multi_chunk-rs1" : {
			"rejectedPlans" : [ ],
			"winningPlan" : [
				{
					"stage" : "PROJECTION_COVERED",
					"transformBy" : {
						"_id" : 0,
						"shardKey" : 1
					}
				},
				{
					"direction" : "forward",
					"indexBounds" : {
						"shardKey" : [
							"[MinKey, MaxKey]"
						]
					},
					"indexName" : "shardKey_1",
					"isFetching" : false,
					"isMultiKey" : false,
					"isPartial" : false,
					"isShardFiltering" : true,
					"isSparse" : false,
					"isUnique" : false,
					"keyPattern" : {
						"shardKey" : 1
					},
					"multiKeyPaths" : {
						"shardKey" : [ ]
					},
					"stage" : "DISTINCT_SCAN"
				}
			]
		}
	}
}
```

## 2. $group on shard key with $top/$bottom
### sort by shard key, output shard key
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"shardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"shardKey" : -1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"shardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"shardKey" : -1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"shardKey" : -1
						}
					}
				}
			}
		}
	]
}
```

### sort by shard key and another field, output shard key
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"shardKey" : 1,
						"notShardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"shardKey" : 1,
						"notShardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### sort by shard key and another field, output non-shard key field
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"shardKey" : 1,
						"notShardKey" : 1
					},
					"output" : "$notShardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "1notShardKey_chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "1notShardKey_chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "1notShardKey_chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "1notShardKey_chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "1notShardKey_chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "1notShardKey_chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "1notShardKey_chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "1notShardKey_chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "1notShardKey_chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "1notShardKey_chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "1notShardKey_chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "1notShardKey_chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "1notShardKey_chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "1notShardKey_chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "1notShardKey_chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "1notShardKey_chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "1notShardKey_chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "1notShardKey_chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"shardKey" : 1,
						"notShardKey" : 1
					},
					"output" : "$notShardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "3notShardKey_chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "3notShardKey_chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "3notShardKey_chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "3notShardKey_chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "3notShardKey_chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "3notShardKey_chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "3notShardKey_chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "3notShardKey_chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "3notShardKey_chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "3notShardKey_chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "3notShardKey_chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "3notShardKey_chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "3notShardKey_chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "3notShardKey_chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "3notShardKey_chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "3notShardKey_chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "3notShardKey_chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "3notShardKey_chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1,
							"shardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### sort by non-shard key field, output shard key
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"notShardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"notShardKey" : 1
					},
					"output" : "$shardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$shardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### sort by non-shard key field, output non-shard key field
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$top" : {
					"sortBy" : {
						"notShardKey" : 1
					},
					"output" : "$notShardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "1notShardKey_chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "1notShardKey_chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "1notShardKey_chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "1notShardKey_chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "1notShardKey_chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "1notShardKey_chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "1notShardKey_chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "1notShardKey_chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "1notShardKey_chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "1notShardKey_chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "1notShardKey_chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "1notShardKey_chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "1notShardKey_chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "1notShardKey_chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "1notShardKey_chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "1notShardKey_chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "1notShardKey_chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "1notShardKey_chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$top" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$top" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$bottom" : {
					"sortBy" : {
						"notShardKey" : 1
					},
					"output" : "$notShardKey"
				}
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "3notShardKey_chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "3notShardKey_chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "3notShardKey_chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "3notShardKey_chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "3notShardKey_chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "3notShardKey_chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "3notShardKey_chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "3notShardKey_chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "3notShardKey_chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "3notShardKey_chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "3notShardKey_chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "3notShardKey_chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "3notShardKey_chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "3notShardKey_chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "3notShardKey_chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "3notShardKey_chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "3notShardKey_chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "3notShardKey_chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_SIMPLE",
						"transformBy" : {
							"_id" : 0,
							"notShardKey" : 1,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"stage" : "SHARDING_FILTER"
					},
					{
						"direction" : "forward",
						"stage" : "COLLSCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$bottom" : {
						"output" : "$$ROOT.accum",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$bottom" : {
						"output" : "$notShardKey",
						"sortBy" : {
							"notShardKey" : 1
						}
					}
				}
			}
		}
	]
}
```

## 3. $group on shard key with $first/$last
### with preceding $sort on shard key
### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : -1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$first" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$first" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"shardKey" : -1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : 1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$first" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$first" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"shardKey" : 1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : -1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$last" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "forward",
							"indexBounds" : {
								"notShardKey" : [
									"[MinKey, MaxKey]"
								],
								"shardKey" : [
									"[MinKey, MaxKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$last" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"shardKey" : -1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : 1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$last" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [
					[
						{
							"stage" : "PROJECTION_COVERED",
							"transformBy" : {
								"_id" : 0,
								"shardKey" : 1
							}
						},
						{
							"stage" : "SORT_KEY_GENERATOR"
						},
						{
							"direction" : "backward",
							"indexBounds" : {
								"notShardKey" : [
									"[MaxKey, MinKey]"
								],
								"shardKey" : [
									"[MaxKey, MinKey]"
								]
							},
							"indexName" : "shardKey_1_notShardKey_1",
							"isFetching" : false,
							"isMultiKey" : false,
							"isPartial" : false,
							"isShardFiltering" : true,
							"isSparse" : false,
							"isUnique" : false,
							"keyPattern" : {
								"notShardKey" : 1,
								"shardKey" : 1
							},
							"multiKeyPaths" : {
								"notShardKey" : [ ],
								"shardKey" : [ ]
							},
							"stage" : "DISTINCT_SCAN"
						}
					]
				],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$last" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"shardKey" : 1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	]
}
```

### with preceding $sort on shard key and another field
### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : 1,
			"notShardKey" : 1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$first" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"notShardKey" : [
								"[MinKey, MaxKey]"
							],
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$first" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"notShardKey" : 1,
					"shardKey" : 1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$sort" : {
			"shardKey" : 1,
			"notShardKey" : 1
		}
	},
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$last" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"stage" : "SORT_KEY_GENERATOR"
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"notShardKey" : [
								"[MaxKey, MinKey]"
							],
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1_notShardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"notShardKey" : 1,
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"notShardKey" : [ ],
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$last" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$sort" : {
				"sortKey" : {
					"notShardKey" : 1,
					"shardKey" : 1
				}
			}
		},
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	]
}
```

### without preceding $sort, output shard key
### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$first" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$groupByDistinctScan" : {
				"newRoot" : {
					"_id" : "$shardKey",
					"accum" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"direction" : "forward",
						"indexBounds" : {
							"shardKey" : [
								"[MinKey, MaxKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$groupByDistinctScan" : {
				"newRoot" : {
					"_id" : "$shardKey",
					"accum" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$first" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$first" : "$shardKey"
				}
			}
		}
	]
}
```

### Pipeline
```json
[
	{
		"$group" : {
			"_id" : "$shardKey",
			"accum" : {
				"$last" : "$shardKey"
			}
		}
	}
]
```
### Results
```json
{  "_id" : "chunk1_s0_0",  "accum" : "chunk1_s0_0" }
{  "_id" : "chunk1_s0_1",  "accum" : "chunk1_s0_1" }
{  "_id" : "chunk1_s0_2",  "accum" : "chunk1_s0_2" }
{  "_id" : "chunk1_s1_0",  "accum" : "chunk1_s1_0" }
{  "_id" : "chunk1_s1_1",  "accum" : "chunk1_s1_1" }
{  "_id" : "chunk1_s1_2",  "accum" : "chunk1_s1_2" }
{  "_id" : "chunk2_s0_0",  "accum" : "chunk2_s0_0" }
{  "_id" : "chunk2_s0_1",  "accum" : "chunk2_s0_1" }
{  "_id" : "chunk2_s0_2",  "accum" : "chunk2_s0_2" }
{  "_id" : "chunk2_s1_0",  "accum" : "chunk2_s1_0" }
{  "_id" : "chunk2_s1_1",  "accum" : "chunk2_s1_1" }
{  "_id" : "chunk2_s1_2",  "accum" : "chunk2_s1_2" }
{  "_id" : "chunk3_s0_0",  "accum" : "chunk3_s0_0" }
{  "_id" : "chunk3_s0_1",  "accum" : "chunk3_s0_1" }
{  "_id" : "chunk3_s0_2",  "accum" : "chunk3_s0_2" }
{  "_id" : "chunk3_s1_0",  "accum" : "chunk3_s1_0" }
{  "_id" : "chunk3_s1_1",  "accum" : "chunk3_s1_1" }
{  "_id" : "chunk3_s1_2",  "accum" : "chunk3_s1_2" }
```
### Summarized explain
```json
{
	"distinct_scan_multi_chunk-rs0" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$groupByDistinctScan" : {
				"newRoot" : {
					"_id" : "$shardKey",
					"accum" : "$shardKey"
				}
			}
		}
	],
	"distinct_scan_multi_chunk-rs1" : [
		{
			"$cursor" : {
				"rejectedPlans" : [ ],
				"winningPlan" : [
					{
						"stage" : "PROJECTION_COVERED",
						"transformBy" : {
							"_id" : 0,
							"shardKey" : 1
						}
					},
					{
						"direction" : "backward",
						"indexBounds" : {
							"shardKey" : [
								"[MaxKey, MinKey]"
							]
						},
						"indexName" : "shardKey_1",
						"isFetching" : false,
						"isMultiKey" : false,
						"isPartial" : false,
						"isShardFiltering" : true,
						"isSparse" : false,
						"isUnique" : false,
						"keyPattern" : {
							"shardKey" : 1
						},
						"multiKeyPaths" : {
							"shardKey" : [ ]
						},
						"stage" : "DISTINCT_SCAN"
					}
				]
			}
		},
		{
			"$groupByDistinctScan" : {
				"newRoot" : {
					"_id" : "$shardKey",
					"accum" : "$shardKey"
				}
			}
		}
	],
	"mergeType" : "mongos",
	"mergerPart" : [
		{
			"$mergeCursors" : {
				"allowPartialResults" : false,
				"compareWholeSortKey" : false,
				"nss" : "test.distinct_scan_multi_chunk",
				"recordRemoteOpWaitTime" : false,
				"requestQueryStatsFromRemotes" : false,
				"tailableMode" : "normal"
			}
		},
		{
			"$group" : {
				"$doingMerge" : true,
				"_id" : "$$ROOT._id",
				"accum" : {
					"$last" : "$$ROOT.accum"
				}
			}
		}
	],
	"shardsPart" : [
		{
			"$group" : {
				"_id" : "$shardKey",
				"accum" : {
					"$last" : "$shardKey"
				}
			}
		}
	]
}
```

