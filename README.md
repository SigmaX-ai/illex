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
illex file examples/tripreport.as -s 0 --pretty
```

Without supplying an output file with `-o`, the output is written to stdout.
This results in the following output:

```
{
    "timestamp": "2005-09-09T10:58:06-10:00",
    "timezone": 882,
    "vin": 16852419560058650624,
    "odometer": 196,
    "hypermiling": false,
    "avgspeed": 63,
    "sec_in_band": [3262, 3403, 893, 2224, 78, 162, 2332, 1473, 2587, 3446, 178, 997],
    "miles_in_time_range": [2403, 3375, 2552, 2146, 919, 2240, 1043, 1079, 3750, 1665, 2062, 46, 2868, 374, 3304, 4108, 3318, 627, 3523, 2225, 357, 1653, 2757, 3476],
    "const_speed_miles_in_band": [3549, 4174, 2541, 2840, 157, 2922, 651, 315, 2483, 2696, 165, 1366],
    "vary_speed_miles_in_band": [958, 2501, 155, 1516, 1208, 2228, 1850, 4031, 3224, 2704, 2063, 484],
    "sec_decel": [3072, 722, 2548, 547, 3467, 843, 3064, 2709, 1515, 763],
    "sec_accel": [2972, 2580, 3829, 792, 2406, 2424, 3304, 2985, 1920, 3889],
    "braking": [909, 2540, 13, 3532, 59, 116],
    "accel": [134, 1780, 227, 1266, 2388, 436],
    "orientation": false,
    "small_speed_var": [1879, 1253, 3048, 376, 754, 1745, 3665, 2820, 3302, 2557, 1308, 2794, 941],
    "large_speed_var": [2048, 3701, 931, 2040, 3387, 2575, 881, 1821, 3674, 2079, 3972, 4132, 3964],
    "accel_decel": 4165,
    "speed_changes": 1148
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
