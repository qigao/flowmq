#include "flowmq_protocol.h"
#include "flowmq_security.h"

#include "turbo_error.h"
#include "turbo_str.h"

#include <stdio.h>
#include <string.h>

int main(void) {
  static const char channel_binding[FLOWMQ_SECURITY_CHANNEL_BINDING_SIZE] = {1};
  flowmq_security_t input;
  flowmq_security_t output;
  tstr encoded = NULL;
  int rc;

  memset(&input, 0, sizeof(input));
  memset(&output, 0, sizeof(output));
  input.mode = FLOWMQ_SECURITY_AUTH;
  input.identity = vstr_from_cstr("client-a");
  input.method = vstr_from_cstr("token");
  input.secret = vstr_from_cstr("example-credential");
  input.channel_binding = vstr_from_buf(channel_binding, sizeof(channel_binding));

  rc = flowmq_security_encode(&input, &encoded);
  if (rc == TURBO_OK) rc = flowmq_security_decode(tstr_to_v(encoded), &output);
  if (rc != TURBO_OK) {
    fprintf(stderr, "FlowMQ security HELLO round-trip failed: %d\n", rc);
    tstr_free(encoded);
    return 1;
  }

  printf("identity=%.*s method=%.*s binding=%zu bytes\n", (int)output.identity.len,
         output.identity.data, (int)output.method.len, output.method.data,
         output.channel_binding.len);
  tstr_free(encoded);
  return 0;
}
