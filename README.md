# TinyDB

I'm borrowing code from bustub as begining for the overall code architecture, later i will build new modules and replace the original one.

The initial design intuition is i'm trying to build a dbms that decouple the transaction layer, execution layer and storage layer. And my goal is to support tpcc test and i want to figure out how slow it will be if i didn't aware the performance issue while coding.