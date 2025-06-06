# Agent Guideline

## Repository purpose

1. This is a repository for streaming coin exchange websocket streams into clickhouse database.
2. Requires high throughput and low latency.
3. Aims to maintain a high level of code quality via ergonomic and idiomatic Rust code.
4. Documentation is not necessary for every function, but should be provided for complex or core logic.

## Developer profile

This is a list of developers who have expressed interest in contributing to the project.
Each entry includes their contact information, programming experience, and any specific areas of expertise.

- totorovirus@gmail.com: Recently began programming in Rust, has experience with Python, and knows well how to manage memories
since he is experienced in hacking and reverse engineering.

## General Guidelines

1. Use ergonomic and idiomatic Rust code.
2. If you introduce a new dependency, please ensure it is well-maintained and widely used. Notify the packages you added to the user.
3. Add integration tests for any websocket streams you implement. Tag it ignored so that it does not run in CI.
4. The code should be well-structured and modular, allowing for easy maintenance and extension.
5. Keep in mind the purpose of the repository: high throughput and low latency for streaming coin exchange websocket streams into a ClickHouse database.

## Instructions from the user

1. User gives a reference documentation for relevant websocket streams in this format:
  - Documentation: <URL> <short description of the documentation>
2. When user provides a module to reference read carefully and try to keep the code style and structure of the module. If you see obvious or necessary improvements,
please suggest them but do not make changes without user approval.
