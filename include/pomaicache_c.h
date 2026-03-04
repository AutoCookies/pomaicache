#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct pomai_t pomai_t;

typedef struct {
  uint64_t memory_limit_bytes;
  const char *data_dir;
} pomai_config_t;

typedef struct {
  uint8_t hit;
  uint64_t cached_tokens;
  uint64_t suffix_tokens;
  double savings_ratio;
} pomai_prompt_result_t;

pomai_t *pomai_create(const pomai_config_t *cfg);
void pomai_destroy(pomai_t *db);

int pomai_set(pomai_t *db,
              const char *key, size_t key_len,
              const void *value, size_t value_len,
              uint64_t ttl_ms);

int pomai_get(pomai_t *db,
              const char *key, size_t key_len,
              void **out_value, size_t *out_len);

void pomai_free(void *ptr);

int pomai_prompt_put(pomai_t *db,
                     const uint64_t *tokens, size_t len,
                     const void *artifact, size_t artifact_len,
                     uint64_t ttl_ms);

int pomai_prompt_get(pomai_t *db,
                     const uint64_t *tokens, size_t len,
                     pomai_prompt_result_t *out);

#ifdef __cplusplus
}
#endif

