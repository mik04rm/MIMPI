/**
 * This file is for implementation of mimpirun program.
 * */

#include "channel.h"
#include "mimpi_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static const int INT_BUF_SIZE = 15;

static void setenv_int(char *name, int val) {
    char buff[INT_BUF_SIZE];
    snprintf(buff, sizeof(buff), "%d", val);
    ASSERT_SYS_OK(setenv(name, buff, 1));
}

int main(int argc, char *argv[]) {

    int world_size = atoi(argv[1]);

    setenv_int("MIMPI_WORLD_SIZE", world_size);

    char *prog_name = argv[2];

    char **prog_args = &argv[2];

    for (int i = 20; i <= 1023; i++) {
        close(i);
    }

    // channels[i][j] is two-element array of descriptors used for prog with
    // rank `i` to read and for prog with rank 'j' to write
    int channels[world_size][world_size][2];

    int next_free = 20;

    for (int i = 0; i < world_size; i++) {
        for (int j = 0; j < world_size; j++) {
            if (i == j) {
                continue;
            }
            ASSERT_SYS_OK(channel(channels[i][j]));

            // we want `b` to be initialized to an index of the smaller
            // descriptor
            int b = channels[i][j][0] < channels[i][j][1] ? 0 : 1;
            for (int k = 0; k < 2; k++) {
                if (channels[i][j][b] != next_free) {
                    ASSERT_SYS_OK(dup2(channels[i][j][b], next_free));
                    ASSERT_SYS_OK(close(channels[i][j][b]));
                    channels[i][j][b] = next_free;
                }
                next_free++;
                b = 1 - b;
            }
        }
    }

    for (int rank = 0; rank < world_size; rank++) {
        pid_t pid;
        ASSERT_SYS_OK(pid = fork());
        if (pid == 0) {

            for (int i = 0; i < world_size; i++) {
                for (int j = 0; j < world_size; j++) {
                    if (i == j || i == rank || j == rank) {
                        continue;
                    }
                    ASSERT_SYS_OK(close(channels[i][j][0]));
                    ASSERT_SYS_OK(close(channels[i][j][1]));
                }
            }

            for (int i = 0; i < world_size; i++) {
                if (i == rank) {
                    continue;
                }

                ASSERT_SYS_OK(close(channels[i][rank][0]));
                ASSERT_SYS_OK(close(channels[rank][i][1]));

                char dsc_str[ENVNAME_BUF_SIZE];

                snprintf(dsc_str, sizeof(dsc_str), "MIMPI_READ_DSC_%d", i);
                setenv_int(dsc_str, channels[rank][i][0]);

                snprintf(dsc_str, sizeof(dsc_str), "MIMPI_WRITE_DSC_%d", i);
                setenv_int(dsc_str, channels[i][rank][1]);
            }
            setenv_int("MIMPI_RANK", rank);

            // prog_name can be in PATH
            ASSERT_SYS_OK(execvp(prog_name, prog_args));
        }
    }

    for (int i = 0; i < world_size; i++) {
        for (int j = 0; j < world_size; j++) {
            if (i == j) {
                continue;
            }
            ASSERT_SYS_OK(close(channels[i][j][1]));
            ASSERT_SYS_OK(close(channels[i][j][0]));
        }
    }

    for (int i = 0; i < world_size; i++) {
        pid_t pid;
        ASSERT_SYS_OK(pid = wait(NULL));
    }
    return 0;
}