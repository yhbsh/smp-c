# smp-c

Single-threaded, non-blocking media relay server in pure C.

Uses `kqueue` on macOS and `epoll` on Linux. Zero dependencies beyond libc.

## Protocol

SMP (Simple Media Protocol) is a lightweight binary protocol for real-time media relay.

```
Client → Server (handshake):
  "SMP0" (4B) | version (1B) | mode (1B: 0=pull, 1=push) | path length (2B) | path (NB) | session id (16B)

Server → Client (response):
  "SMP0" (4B) | version (1B) | status (1B: 0=ok, 1=bad handshake, 2=occupied)

Data stream (after handshake):
  length (4B) | type (1B: 0x01=header, 0x02=packet, 0x03=keyframe) | payload
```

Publishers push media to a path. Subscribers pull from the same path. The server fans out packets to all subscribers with zero-copy reference counting.

New subscribers catch up instantly: they receive the cached stream header + current GOP (group of pictures), so join latency equals one keyframe interval.

## Build

```
make
```

## Usage

```
./bin/smp [port]    # default: 7777
```

Push and pull with FFmpeg:

```
ffmpeg -re -i input.mp4 -c copy -f smp smp://localhost:7777/live
ffplay -f smp smp://localhost:7777/live
```
