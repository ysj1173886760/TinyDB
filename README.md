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
- [ ] table heap
- [ ] catalog
- [ ] index
- [ ] expression
- [ ] execution
- [ ] parser
- [ ] planner
- [ ] transaction