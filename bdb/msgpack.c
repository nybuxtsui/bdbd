#include "msgpack.h"
#include <string.h>
#include <stdio.h>

bool
msgpack_reader(cmp_ctx_t *ctx, void *data, size_t limit) {
    struct msgpack_ctx *reader = (struct msgpack_ctx*)ctx->buf;
    if (reader->pos + limit > reader->len) {
        printf("msgpack_reader err\n");
        return false;
    }
    memcpy(data, reader->buf + reader->pos, limit);
    reader->pos += limit;
    return true;
}

size_t
msgpack_writer(cmp_ctx_t *ctx, const void *data, size_t count) {
    struct msgpack_ctx *writer = (struct msgpack_ctx*)ctx->buf;
    if (writer->pos + count > writer->len) {
        printf("msgpack_writer err\n");
        return 0;
    }
    memcpy(writer->buf + writer->pos, data, count);
    writer->pos += count;
    return count;
}
