# TinyDB

I'm borrowing code from bustub as begining for the overall code architecture, later i will build new modules and replace the original one.

The initial design intuition is i'm trying to build a dbms that decouple the transaction layer, execution layer and storage layer. And my goal is to support tpcc test and i want to figure out how slow it will be if i didn't aware the performance issue while coding.

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
- [ ] execution
- [ ] transaction
- [ ] logger
- [ ] finish database kernel
- [ ] parser
- [ ] planner
- [ ] support TPCC

# Important TODOs

- [ ] Deallocate page in disk (use bitmap to manage page allocation)
- [ ] Delete empty pages in table heap (essentially it's a concurrent doubly-linked list)
- [ ] Implement variable-length data pool. (currently, i stored it right after the tuple, which leads to the varied-length tuple. And when we want to perform updation of a tuple, we might fail since table might not have enough space for new tuple, thus we need to perform an deletion followed by an insertion, which may introduce more engineering overhead)
- [ ] B+Tree may still contains bugs, especially when handling deleted pages, pinned pages and dirty pages. After we've implemented page management, we shall use it to check whether B+Tree will give the deleted page back safely.