.PHONY: all clean

EXAMPLES := $(addprefix examples_build/,$(notdir $(basename $(wildcard examples/*.c))))
CFLAGS := --std=gnu11 -Wall -DDEBUG -pthread
TESTS := $(wildcard tests/*.self)

CHANNEL_SRC := channel.c channel.h
MIMPI_COMMON_SRC := $(CHANNEL_SRC) mimpi_common.c mimpi_common.h
MIMPIRUN_SRC := $(MIMPI_COMMON_SRC) mimpirun.c
MIMPI_SRC := $(MIMPI_COMMON_SRC) mimpi.c mimpi.h

all: mimpirun $(EXAMPLES) $(TESTS)

mimpirun: $(MIMPIRUN_SRC)
	gcc $(CFLAGS) -o $@ $(filter %.c,$^)

examples_build/%: examples/%.c $(MIMPI_SRC)
	mkdir -p examples_build
	gcc $(CFLAGS) -o $@ $(filter %.c,$^)

clean:
	rm -rf mimpirun examples_build
