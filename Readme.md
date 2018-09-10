# Zero

Fuse filesystem backed by backblaze cloud storage and transparent local persistent hard-drive cache.

Ideally, it feels like an infinite local file system because it keeps those files local that are used while moving those files to the remote storage that haven't been accessed in a long time.

## State of Development
**This is Work in Progress**

There are a few *known issues and race conditions*. For example, the ctime on the files may be newer than the correct value. Also, writes and reads are *slow*.
*Do not use in production.*

However, *the code is a mess* and needs to be refactored and cleaned up significantly.

This, in turn, requires better test coverage. Thus, the next step to improve this software is by extending test coverage.

## Setup

Create a config.yml:
```
accountId: [...]
applicationKey: [...]
bucketId: [...]
sqliteFileLocation: [...]
```
and save in `~/.config/zero/`

Here, `accountId`, `applicationKey` and `bucketId` are the corresponding backblaze settings and `sqliteFileLocation` is simply the path to a place where the sqlite databases containing the state of the virtual file system can be stored.


## Testing

Run the tests with `pytest` like this:
`py.test`

There are 3 categories of tests:
- Unit tests are testing the code on the function level
- Integration tests are testing the interactions between bigger parts of the code and outside APIs. They may use the internet.
- System tests are testing the software as a black box. They should continue working through a big refactor or even if everything was re-written in a different programming language. (There are no system tests yet as of now.)
