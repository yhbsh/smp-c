# smp-c

SMP media relay server in pure C. Single-threaded, non-blocking (`kqueue` on macOS, `epoll` on Linux). Zero dependencies beyond libc.

## Build

```
make
```

## Usage

```
./bin/smp [port]    # default: 7777
```

Push and pull with the [SMP-enabled FFmpeg fork](https://github.com/yhbsh/ffmpeg):

```
ffmpeg -re -i input.mp4 -c copy -f smp smp://localhost:7777/live
ffplay -f smp smp://localhost:7777/live
```
