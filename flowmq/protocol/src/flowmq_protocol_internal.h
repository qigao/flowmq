#ifndef FLOWMQ_PROTOCOL_INTERNAL_H
#define FLOWMQ_PROTOCOL_INTERNAL_H

#include "flowmq_protocol.h"

int flowmq_protocol_encode_frame_into_internal(
    const flowmq_protocol_frame_t *frame, size_t max_frame_size, void *storage,
    size_t storage_size, size_t *encoded_size);

#endif /* FLOWMQ_PROTOCOL_INTERNAL_H */
