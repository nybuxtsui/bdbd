#ifndef __MSGPACK_H__
#define __MSGPACK_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "cmp.h"

struct msgpack_ctx {
    char *buf;
    int pos;
    int len;
};

bool msgpack_reader(cmp_ctx_t *ctx, void *data, size_t limit);
size_t msgpack_writer(cmp_ctx_t *ctx, const void *data, size_t count);

#endif
