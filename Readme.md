# Zero

Fuse filesystem backed by backblaze cloud storage and transparent local persistent hard-drive cache.

Ideally, it feels like an infinite local file system because it keeps those files local that are used while moving those files to the remote storage that haven't been accessed in a long time.

## Hard links are not supported

Zero operates on a abstraction of files being addressed by paths.
In order to offer hard good hard link support, one would need to instead choose an inode abstraction level, where multiple paths map to the same inode, which then addresses the file.

Unfortunately, fuse does not seem to support a true inode-level API. For example an inode-level API would not require to implement a `move` operation, as this would be an operation handled on the path->inode mapping level.

One could imagine building an adapter that translates from fuse into an inode-level API.

This may be a project for a later date.

## State of Development

**This is WORK IN PROGRESS**

There are a few _known issues and race conditions_. For example, the ctime on the files may be newer than the correct value. Also, writes and reads are _slow_.
_Do not use in production._

## Setup

Create a config.yml:

```
accountId: [...]
applicationKey: [...]
bucketId: [...]
sqliteFileLocation: [...]
targetDiskUsage: [...]
```

and save in `~/.config/zero/`

Here, `accountId`, `applicationKey` and `bucketId` are the corresponding backblaze settings and `sqliteFileLocation` is simply the path to a place where the sqlite databases containing the state of the virtual file system can be stored.
`targetDiskUsage` is the amount of disk space (in GB) that you would like to use for local caching.

Install with `python setup.py develop`

## Usage

Run

    zero-fuse <mountpoint> <cache-location>

to mount the file system.
You will also need to run

    zero-worker <cache-location>

to start the worker process that moves files between disk and cloud.

## Testing

Run the tests with `pytest` like this:
`py.test`

There are 3 categories of tests:

- Unit tests are testing the code on the function level
- Integration tests are testing the interactions between bigger parts of the code and outside APIs. They may use the internet.
- System tests are testing the software as a black box. They should continue working through a big refactor or even if everything was re-written in a different programming language. (There are no system tests yet as of now.)
