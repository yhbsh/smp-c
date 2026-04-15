CC     := cc
CFLAGS := -std=c11 -O2 -Wall -Wextra -Wpedantic

default: build

build:
	@mkdir -p bin
	$(CC) $(CFLAGS) -o bin/smp main.c

clean:
	rm -f bin/smp

.PHONY: default build run clean
