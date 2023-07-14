# deltalake-redis-lock

A library creating an interface for a write lock for [delta-rs](https://pypi.org/project/deltalake/).

## Library Usage

#### Without providing loop
```python

```

## Setup From Scratch

### Requirement

* ^python3.8
* poetry 1.1.13
* make (GNU Make 3.81)

### Setup

```bash
make setup-environment
```

Update package
```bash
make update
```

### Test

```bash
make test type=unit/integration
```

### Docker

The reason `docker` is used in the source code here, is to be able to build up an encapsulated
environment of the codebase, and do `unit/integration and load tests`.

```bash
make build-container-image
```

```bash
make get-container-info-environment
make run-container-tests type=unit
```
