# Illex

A tool to generate random JSONs based on [Arrow](https://arrow.apache.org)
Schemas.

## Build & install

- Requirements
    - To build:
        - CMake 3.14+
        - A C++17 compiler.
    - Dependencies:
        - [Arrow 3.0.0](https://arrow.apache.org)

### Build

```bash
git clone https://github.com/teratide/illex.git
cd illex
mkdir build && cd build
cmake ..
make
```

### Install

After building:

```bash
make install
```

### Docker image

A Dockerfile is provided to build a Docker image that runs Illex:

```bash
docker build -t illex .
```

### Arrow Schema examples

Illex requires an Arrow schema to generate JSONs. You can generate the examples
in the [examples folder](examples) by running:

```bash
cd examples
./generate_schemas.py
```

## Usage

There are two subcommands, `file` and `stream`.

Both subcommand require an Arrow schema to be supplied as the first positional
argument, or through `-i` or `--input`.

More detailed options can be found by running:

```
illex --help
```

or:

```
illex <subcommand> --help
```

### File subcommand

After generating the Arrow Schema examples, you could run:

```bash
illex file examples/basics.as -s 0 --pretty
```

Without supplying an output file with `-o`, the output is written to stdout.
This results in the following output:

```
{
    "timestamp": "2005-09-09T10:58:06-10:00",
    "string": "weyhtufnaanip",
    "integer": 32,
    "list_of_strings": ["oupm", "nggw", "marcty"],
    "bool": true
}
```

Or using a Docker container:

```bash
docker run --rm -it -v `pwd`:/io illex file /io/examples/tripreport.as -s 0 --pretty
```

### Stream subcommand

To start a TCP server streaming out 1048576 JSONs to a client, using 8 threads
to generate the JSONs:

```bash
illex stream examples/battery.as -n 1048576 -t 8
```

Or using a Docker container:

```bash
docker run --rm -it -p 10197:10197 -v `pwd`:/io illex stream /io/examples/battery.as -n 1048576 -t 8
```

## FAQ

- Why is it named Illex?
    - Because this thing spouts random JSONs, it's called Illex, like the little
      squid species. It's a working title.
