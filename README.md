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
    "timestamp": "2005-09-09T11:59:06-10:00",
    "timezone": 883,
    "vin": 16852243674679352615,
    "odometer": 997,
    "hypermiling": false,
    "avgspeed": 156,
    "sec_in_band": [3403, 893, 2225, 78, 162, 2332, 1473, 2587, 3446, 178, 997, 2403],
    "miles_in_time_range": [3376, 2553, 2146, 919, 2241, 1044, 1079, 3751, 1665, 2062, 46, 2868, 375, 3305, 4109, 3319, 627, 3523, 2225, 357, 1653, 2757, 3477, 3549],
    "const_speed_miles_in_band": [4175, 2541, 2841, 157, 2922, 651, 315, 2484, 2696, 165, 1366, 958],
    "vary_speed_miles_in_band": [2502, 155, 1516, 1208, 2229, 1850, 4032, 3225, 2704, 2064, 484, 3073],
    "sec_decel": [722, 2549, 547, 3468, 844, 3064, 2710, 1515, 763, 2972],
    "sec_accel": [2580, 3830, 792, 2407, 2425, 3305, 2985, 1920, 3889, 909],
    "braking": [2541, 13, 3533, 59, 116, 134],
    "accel": [1780, 228, 1267, 2389, 437, 871],
    "orientation": false,
    "small_speed_var": [1254, 3048, 377, 754, 1745, 3666, 2820, 3303, 2558, 1308, 2795, 941, 2049],
    "large_speed_var": [3702, 931, 2040, 3388, 2575, 881, 1821, 3675, 2080, 3973, 4132, 3965, 4166],
    "accel_decel": 1148,
    "speed_changes": 1932
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
illex stream examples/battery.as -m 1048576 -t 8
```

Or using a Docker container:

```bash
docker run --rm -it -p 10197:10197 -v `pwd`:/io illex stream /io/examples/battery.as -m 1048576 -t 8
```

## FAQ

- Why is it named Illex?
  - Because this thing spouts random JSONs, it's called Illex, like the little
    squid species. It's a working title.
