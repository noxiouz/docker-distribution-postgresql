## docker-distribution-postgresql [![Build Status](https://travis-ci.org/noxiouz/docker-distribution-postgresql.svg?branch=master)](https://travis-ci.org/noxiouz/docker-distribution-postgresql) [![codecov.io](https://codecov.io/github/noxiouz/docker-distribution-postgresql/coverage.svg?branch=master)](https://codecov.io/github/noxiouz/docker-distribution-postgresql?branch=master)
This driver stores metadata for files in PostgreSQL and binary data in a KV storage. Currently only Yandex specific KV is supported.

### Configuration

```yaml
storage:
    postgres:
        URLs:
          - "postgres://noxiouz@localhost:5432/distribution?sslmode=disable"
        MaxOpenConns: 10
        MaxIdleConns: 5
        type: "mds"
        options:
            host: "mdshost.yandex.net"
            uploadport: 1111
            readport: 80
            authheader: "Basic <basic auth header>"
            namespace: "some-namepace"
```

### Limitations

As Yandex MDS does not support append by default, so StreamWrite with non-zero offset leads to an error. It means that resumable uploads will never be supported by MDS backend.

### Status

Non-production ready yet
