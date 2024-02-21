# Cache library for Go

[![Go](https://github.com/dsh2dsh/expx-cache/actions/workflows/go.yml/badge.svg)](https://github.com/dsh2dsh/expx-cache/actions/workflows/go.yml)

Right now this project is under constraction.

This project was started as a fork of
[go-cache](https://github.com/go-redis/cache), but later I decided to rewrite
it.

## How to test

For launching all tests a redis instance required

```
WITH_REDIS="redis://localhost:6379" make test
```

or create file named `.env.local` with next content

```
WITH_REDIS=redis://localhost:6379
```

and run tests by

```
make test
```

If this redis instance is in production, tests should be pointed to another
redis db on this instance, like

```
WITH_REDIS=redis://localhost:6379/1
```

In this case tests will use first and next redis databases (assuming db 0
contains production data). Tests use more databases, starting from db of
connection.
