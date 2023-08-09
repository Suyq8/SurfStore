# SurfStore

Surfstore is a cloud-based file storage service built on the RAFT Protocol, providing a reliable and consistent platform for file synchronization similar to Dropbox. Clients interact with the service via gRPC, allowing multiple clients to concurrently access a shared set of files.

Please note that while SurfStore ensures consistent updates to files for clients, it does not support multi-file transactions such as atomic move operations.

## Services

The Fault-Tolerant Surfstore system is composed of two core services:

### BlockStore

The BlockStore service plays a critical role in the SurfStore ecosystem. Each file's content is divided into distinct chunks, known as blocks, each with a unique identifier. The BlockStore service is responsible for storing these blocks and efficiently retrieving the appropriate block when given its identifier. To distribute the blocks efficiently, SurfStore employs consistent hashing to upload blocks to their corresponding block servers.

### MetaStore

The MetaStore service manages the metadata of files and the overall system. Its key responsibilities include:

- Maintaining the mapping of filenames to blocks, ensuring efficient file retrieval.
- Tracking available BlockStores and mapping blocks to specific BlockStores for optimal distribution and load balancing.

## Getting Started

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```
