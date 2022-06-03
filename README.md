# TinyDB

The initial version of database kernel is done, currently we have a single version storage, with 2PL concurrency control and ARIES recovery algorithm.

The initial design intuition is i'm trying to build a dbms that decouple the transaction layer, execution layer and storage layer. And my goal is to support tpcc test and i want to figure out how slow it will be if i didn't aware the performance issue while coding.

Next step would be to implement more modules. e.g. multi-version storage, other concurrency control algorithm, front-end of database and support tpcc test.

# Implementation Map

- [x] common utils. i.e. logger, rwlatch, exception, macros
- [x] disk manager
- [x] buffer pool manager
- [x] type subsystem
- [x] schema
- [x] tuple
- [x] table page
- [x] table heap
- [x] index
- [x] catalog
- [x] expression
- [x] execution
- [x] transaction
- [x] logger
- [x] finish database kernel
- [ ] parser
- [ ] planner
- [ ] support TPCC

# Important TODOs

- [ ] Deallocate page in disk (use bitmap to manage page allocation)
- [ ] Delete empty pages in table heap (essentially it's a concurrent doubly-linked list)
- [ ] Implement variable-length data pool. (currently, i stored it right after the tuple, which leads to the varied-length tuple. And when we want to perform updation of a tuple, we might fail since table might not have enough space for new tuple, thus we need to perform an deletion followed by an insertion, which may introduce more engineering overhead)
- [ ] B+Tree may still contains bugs, especially when handling deleted pages, pinned pages and dirty pages. After we've implemented page management, we shall use it to check whether B+Tree will give the deleted page back safely.
- [ ] Figure out how to manage expression tree, currently i just stored the raw pointer, and delete all expressions i've created at the end of scope. Storing raw pointer allows us to reuse the expression, but makes creating expression tree and freeing it more complicated. So maybe we should use something like unique_pointer to manage expression tree just like what i did in executor.
- [ ] Find a way to automatically generate tuples and tables that can support strong tests. Currently i just hardcode the tuple value. Or maybe we can construct some ad-hoc test cases, i.e. table for join only, table for updation only.
- [ ] Currently, we cann't handle very correctly the faiure while some transaction is committing. This could be resolved by multi-version storage engine since we can perform deletion in background. In current implementation, since i'm performing in-place deletion, there is no good way to handle the atomic deletion. A possible appoach would be to use some special mark and transaction id to indicate the tuple was deleted, and launch background thread periodically to perform garbage collection.
- [ ] Some module are not test coverage is not enough, we still need more unit test to make sure our component is strong enough.

# Design Choices

* Currently, i decide to only support adding new pages for table heap, but not to support deleting pages inside table heap. It's because i was using a linked-list to represent table heap, which is hard to guarantee persistent property and handling concurrent issues.
* For logging, currently, i'm planning only support recovery from empty database. i.e. no checkpointing. And this will simplify some implementation. After we've support logging for all metadata, e.g. disk allocation, table heap, catalog, then we can move on to support checkpointing.