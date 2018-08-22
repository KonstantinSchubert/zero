# Zero

Fuse filesystem backed by cloud storage and transparent local persistent cache


## Setup

Create a config.yml:
```
accountId: [...]
applicationKey: [...]
bucketId: [...]
sqliteFileLocation: [...]
```
and save in `~/.config/zero/`

## Architecture

Not quite there yet, but here is a draft of the planned class diagram:

https://www.lucidchart.com/invitations/accept/02e8f84e-e178-42a7-9e9f-64d3cd5ed3fb

## Testing

Run the tests with `pytest` like this:
`py.test`

There are 3 categories of tests:
- Unit tests are testing the code on the function level
- Integration tests are testing the interactions between bigger parts of the code and outside APIs. They may use the internet.
- System tests are testing the software as a black box. They should continue working through a big refactor or even if everything was re-written in a different programming language.