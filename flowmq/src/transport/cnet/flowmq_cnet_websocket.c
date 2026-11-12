#include "flowmq_cnet_websocket.h"

#include <core/base64.h>
#include <core/error_codes.h>
#include <core/platform.h>
#include <core/vstr.h>
#include <gmssl/sha1.h>
#include <llhttp.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWMQ_WS_PROTOCOL "fmq.v6"
#define WS_LITERAL(text) vstr_from_buf((text), sizeof(text) - 1u)

enum { WS_KEY_SIZE = 24u, WS_NONCE_SIZE = 16u };

static int flowmq_ws_accept(const char *key, char *accept) {
  static const char guid[] = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  SHA1_CTX sha1;
  uint8_t digest[20u];
  sha1_init(&sha1);
  sha1_update(&sha1, (const uint8_t *)key, WS_KEY_SIZE);
  sha1_update(&sha1, (const uint8_t *)guid, sizeof(guid) - 1u);
  sha1_finish(&sha1, digest);
  return tn_base64_encode_buf_ex(digest, sizeof(digest), accept,
                                FLOWMQ_WS_ACCEPT_CAPACITY) == TN_BASE64_OK
      ? SALTS_OK : SALTS_EPROTO;
}

int flowmq_ws_client_request(const flowmq_cnet_endpoint_t *endpoint,
                             char *request, size_t capacity, size_t *size,
                             char accept[FLOWMQ_WS_ACCEPT_CAPACITY]) {
  uint8_t nonce[WS_NONCE_SIZE];
  char key[WS_KEY_SIZE + 1u];
  const int status = secure_random(nonce, sizeof(nonce));
  if (status != SALTS_OK) return status;
  if (tn_base64_encode_buf_ex(nonce, sizeof(nonce), key, sizeof(key)) != TN_BASE64_OK)
    return SALTS_EPROTO;
  const bool ipv6 = strchr(endpoint->host, ':') != NULL;
  const int written = snprintf(request, capacity,
      "GET %s HTTP/1.1\r\n"
      "Host: %s%s%s:%u\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Version: 13\r\n"
      "Sec-WebSocket-Key: %s\r\n"
      "Sec-WebSocket-Protocol: " FLOWMQ_WS_PROTOCOL "\r\n\r\n",
      endpoint->path, ipv6 ? "[" : "", endpoint->host, ipv6 ? "]" : "",
      (unsigned)endpoint->port, key);
  if (written < 0 || (size_t)written >= capacity) return SALTS_EMSGSIZE;
  *size = (size_t)written;
  return flowmq_ws_accept(key, accept);
}

int flowmq_ws_server_response(const char *accept, char *response,
                              size_t capacity, size_t *size) {
  const int written = snprintf(response, capacity,
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Accept: %s\r\n"
      "Sec-WebSocket-Protocol: " FLOWMQ_WS_PROTOCOL "\r\n\r\n", accept);
  if (written < 0 || (size_t)written >= capacity) return SALTS_EMSGSIZE;
  *size = (size_t)written;
  return SALTS_OK;
}

const char *flowmq_ws_server_rejection(int status) {
  switch (status) {
  case SALTS_EPROTO:
    return "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
  case SALTS_ENOENT:
    return "HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
  case SALTS_ENOTSUP:
    return "HTTP/1.1 426 Upgrade Required\r\nConnection: Upgrade, close\r\n"
           "Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n";
  case SALTS_EMSGSIZE:
    return "HTTP/1.1 431 Request Header Fields Too Large\r\n"
           "Connection: close\r\nContent-Length: 0\r\n\r\n";
  default:
    return NULL;
  }
}

typedef struct flowmq_ws_headers_s {
  vstr url;
  vstr field;
  vstr value;
  vstr host;
  vstr key;
  vstr version;
  vstr protocol;
  vstr accept;
  bool server;
  bool protocol_offered;
  bool upgrade;
  bool connection_upgrade;
  bool complete;
} flowmq_ws_headers_t;

static int flowmq_ws_span(vstr *span, const char *data, size_t size) {
  if (span->data == NULL) *span = vstr_from_buf(data, size);
  else if (span->data + span->len == data) span->len += size;
  else return -1;
  return 0;
}

static int flowmq_ws_url(llhttp_t *parser, const char *data, size_t size) {
  flowmq_ws_headers_t *headers = parser->data;
  return flowmq_ws_span(&headers->url, data, size);
}

static int flowmq_ws_field(llhttp_t *parser, const char *data, size_t size) {
  flowmq_ws_headers_t *headers = parser->data;
  return flowmq_ws_span(&headers->field, data, size);
}

static int flowmq_ws_value(llhttp_t *parser, const char *data, size_t size) {
  flowmq_ws_headers_t *headers = parser->data;
  return flowmq_ws_span(&headers->value, data, size);
}

static bool flowmq_ws_token(vstr list, vstr wanted, bool ignore_case) {
  while (list.len != 0u) {
    const size_t comma = vstr_find_char(list, ',');
    const vstr token = vstr_trim(vstr_sub(list, 0u,
        comma == VSTR_NPOS ? list.len : comma), " \t");
    if (ignore_case ? vstr_ieq(token, wanted) : vstr_eq(token, wanted))
      return true;
    if (comma == VSTR_NPOS) break;
    list = vstr_sub(list, comma + 1u, list.len - comma - 1u);
  }
  return false;
}

static int flowmq_ws_header_complete(llhttp_t *parser) {
  flowmq_ws_headers_t *headers = parser->data;
  const vstr field = headers->field;
  const vstr value = vstr_trim(headers->value, " \t");
  vstr *single = NULL;
  headers->field = (vstr){0};
  headers->value = (vstr){0};

  if (vstr_ieq(field, WS_LITERAL("Host"))) single = &headers->host;
  else if (vstr_ieq(field, WS_LITERAL("Sec-WebSocket-Key"))) single = &headers->key;
  else if (vstr_ieq(field, WS_LITERAL("Sec-WebSocket-Version"))) single = &headers->version;
  else if (vstr_ieq(field, WS_LITERAL("Sec-WebSocket-Protocol"))) {
    if (headers->server) {
      if (value.len == 0u) return -1;
      /* RFC 6455 permits a request's offers to span multiple field lines. */
      headers->protocol_offered |= flowmq_ws_token(value, WS_LITERAL(FLOWMQ_WS_PROTOCOL), false);
    } else {
      single = &headers->protocol;
    }
  }
  else if (vstr_ieq(field, WS_LITERAL("Sec-WebSocket-Accept"))) single = &headers->accept;
  else if (vstr_ieq(field, WS_LITERAL("Connection")))
    headers->connection_upgrade |= flowmq_ws_token(value, WS_LITERAL("Upgrade"), true);
  else if (vstr_ieq(field, WS_LITERAL("Upgrade")))
    headers->upgrade |= flowmq_ws_token(value, WS_LITERAL("websocket"), true);
  else if (vstr_ieq(field, WS_LITERAL("Transfer-Encoding")) ||
           (!headers->server && vstr_ieq(field, WS_LITERAL("Sec-WebSocket-Extensions"))) ||
           (vstr_ieq(field, WS_LITERAL("Content-Length")) && !vstr_eq(value, WS_LITERAL("0"))))
    return -1;

  if (single != NULL) {
    if (single->data != NULL || value.len == 0u) return -1;
    *single = value;
  }
  return 0;
}

static int flowmq_ws_headers_complete(llhttp_t *parser) {
  flowmq_ws_headers_t *headers = parser->data;
  headers->complete = true;
  return 2; /* Stop precisely at the transition to WebSocket bytes. */
}

static int flowmq_ws_validate_key(vstr key, char *accept) {
  char text[WS_KEY_SIZE + 1u];
  char canonical[WS_KEY_SIZE + 1u];
  if (key.len != WS_KEY_SIZE) return SALTS_EPROTO;
  memcpy(text, key.data, WS_KEY_SIZE);
  text[WS_KEY_SIZE] = '\0';
  const tn_base64_bytes_result_t decoded = tn_base64_decode_ex(text);
  if (!decoded.ok)
    return decoded.error == TN_BASE64_ERR_NO_MEMORY ? SALTS_ENOMEM : SALTS_EPROTO;
  const bool valid = decoded.value.len == WS_NONCE_SIZE &&
      tn_base64_encode_buf_ex(decoded.value.data, decoded.value.len, canonical,
                              sizeof(canonical)) == TN_BASE64_OK &&
      memcmp(canonical, text, sizeof(text)) == 0;
  free(decoded.value.data);
  return valid ? flowmq_ws_accept(text, accept) : SALTS_EPROTO;
}

static int flowmq_ws_validate_version(vstr version) {
  unsigned number = 0u;
  if (version.len == 0u || version.len > 3u ||
      (version.len > 1u && version.data[0] == '0'))
    return SALTS_EPROTO;
  for (size_t i = 0u; i < version.len; ++i) {
    if (version.data[i] < '0' || version.data[i] > '9') return SALTS_EPROTO;
    number = number * 10u + (unsigned)(version.data[i] - '0');
  }
  if (number > 255u) return SALTS_EPROTO;
  return number == 13u ? SALTS_OK : SALTS_ENOTSUP;
}

int flowmq_ws_upgrade_validate(bool server, const char *data, size_t size,
                               const char *path,
                               char accept[FLOWMQ_WS_ACCEPT_CAPACITY]) {
  llhttp_settings_t settings;
  llhttp_t parser;
  flowmq_ws_headers_t headers = {.server = server};
  llhttp_settings_init(&settings);
  settings.on_url = flowmq_ws_url;
  settings.on_header_field = flowmq_ws_field;
  settings.on_header_value = flowmq_ws_value;
  settings.on_header_value_complete = flowmq_ws_header_complete;
  settings.on_headers_complete = flowmq_ws_headers_complete;
  llhttp_init(&parser, server ? HTTP_REQUEST : HTTP_RESPONSE, &settings);
  parser.data = &headers;
  const llhttp_errno_t status = llhttp_execute(&parser, data, size);
  if (status != HPE_PAUSED_UPGRADE || !headers.complete ||
      llhttp_get_error_pos(&parser) != data + size ||
      llhttp_get_http_major(&parser) != 1u || llhttp_get_http_minor(&parser) != 1u ||
      !headers.connection_upgrade || !headers.upgrade)
    return SALTS_EPROTO;

  if (server) {
    if (llhttp_get_method(&parser) != HTTP_GET || headers.host.len == 0u)
      return SALTS_EPROTO;
    const int version_status = flowmq_ws_validate_version(headers.version);
    if (version_status != SALTS_OK) return version_status;
    if (!vstr_eq(headers.url, vstr_from_buf(path, strlen(path)))) return SALTS_ENOENT;
    if (!headers.protocol_offered) return SALTS_EPROTO;
    return flowmq_ws_validate_key(headers.key, accept);
  }
  return llhttp_get_status_code(&parser) == 101 &&
         vstr_eq(headers.protocol, WS_LITERAL(FLOWMQ_WS_PROTOCOL)) &&
         vstr_eq(headers.accept, vstr_from_buf(accept, strlen(accept)))
      ? SALTS_OK : SALTS_EPROTO;
}
