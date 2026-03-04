#include "pomaicache_c.h"
#include "pomaicache.h"

#include <new>
#include <vector>
#include <cstring>

struct pomai_t {
  pomaicache::PomaiCache impl;

  explicit pomai_t(const pomaicache::Config &cfg) : impl(cfg) {}
};

extern "C" {

pomai_t *pomai_create(const pomai_config_t *cfg) {
  if (!cfg)
    return nullptr;
  pomaicache::Config c;
  c.memory_limit_bytes = static_cast<std::size_t>(cfg->memory_limit_bytes);
  if (cfg->data_dir)
    c.data_dir = cfg->data_dir;
  try {
    return new pomai_t(c);
  } catch (...) {
    return nullptr;
  }
}

void pomai_destroy(pomai_t *db) {
  delete db;
}

int pomai_set(pomai_t *db,
              const char *key, size_t key_len,
              const void *value, size_t value_len,
              uint64_t ttl_ms) {
  if (!db || !key || !value)
    return 0;
  std::string_view k(key, key_len);
  auto *bytes = static_cast<const std::byte *>(value);
  std::span<const std::byte> v(bytes, value_len);
  pomaicache::Ttl ttl{ttl_ms};
  return db->impl.Set(k, v, ttl) ? 1 : 0;
}

int pomai_get(pomai_t *db,
              const char *key, size_t key_len,
              void **out_value, size_t *out_len) {
  if (!db || !key || !out_value || !out_len)
    return 0;
  std::string_view k(key, key_len);
  auto v = db->impl.Get(k);
  if (!v.has_value())
    return 0;
  auto &vec = *v;
  auto *buf = new std::byte[vec.size()];
  std::memcpy(buf, vec.data(), vec.size());
  *out_value = buf;
  *out_len = vec.size();
  return 1;
}

void pomai_free(void *ptr) {
  auto *b = static_cast<std::byte *>(ptr);
  delete[] b;
}

int pomai_prompt_put(pomai_t *db,
                     const uint64_t *tokens, size_t len,
                     const void *artifact, size_t artifact_len,
                     uint64_t ttl_ms) {
  if (!db || !tokens || !artifact)
    return 0;
  std::span<const std::uint64_t> t(tokens, len);
  auto *bytes = static_cast<const std::byte *>(artifact);
  std::span<const std::byte> a(bytes, artifact_len);
  pomaicache::Ttl ttl{ttl_ms};
  return db->impl.PromptPut(t, a, ttl) ? 1 : 0;
}

int pomai_prompt_get(pomai_t *db,
                     const uint64_t *tokens, size_t len,
                     pomai_prompt_result_t *out) {
  if (!db || !tokens || !out)
    return 0;
  std::span<const std::uint64_t> t(tokens, len);
  auto r = db->impl.PromptGet(t);
  out->hit = r.hit ? 1 : 0;
  out->cached_tokens = r.cached_tokens;
  out->suffix_tokens = r.suffix_tokens;
  out->savings_ratio = r.savings_ratio;
  return 1;
}

} // extern "C"

