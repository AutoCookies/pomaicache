#include "pomaicache_c.h"

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

namespace py = pybind11;

namespace {

class PyCache {
public:
  explicit PyCache(const std::string &data_dir, std::uint64_t memory_limit_bytes) {
    pomai_config_t cfg{};
    cfg.memory_limit_bytes = memory_limit_bytes;
    cfg.data_dir = data_dir.c_str();
    handle_ = pomai_create(&cfg);
    if (!handle_) {
      throw std::runtime_error("pomai_create failed");
    }
  }

  ~PyCache() {
    if (handle_)
      pomai_destroy(handle_);
    handle_ = nullptr;
  }

  void prompt_put(const std::vector<std::uint64_t> &tokens,
                  py::buffer artifact,
                  std::uint64_t ttl_ms) {
    py::buffer_info info = artifact.request();
    if (info.ndim != 1) {
      throw std::runtime_error("artifact must be 1D buffer");
    }
    auto *data = static_cast<const void *>(info.ptr);
    const auto len = static_cast<std::size_t>(info.size * info.itemsize);
    if (!pomai_prompt_put(handle_, tokens.data(), tokens.size(), data, len,
                          ttl_ms)) {
      throw std::runtime_error("pomai_prompt_put failed");
    }
  }

  py::dict prompt_get(const std::vector<std::uint64_t> &tokens) {
    pomai_prompt_result_t out{};
    if (!pomai_prompt_get(handle_, tokens.data(), tokens.size(), &out)) {
      throw std::runtime_error("pomai_prompt_get failed");
    }
    py::dict d;
    d["hit"] = static_cast<bool>(out.hit);
    d["cached_tokens"] = out.cached_tokens;
    d["suffix_tokens"] = out.suffix_tokens;
    d["savings_ratio"] = out.savings_ratio;
    return d;
  }

private:
  pomai_t *handle_{nullptr};
};

} // namespace

PYBIND11_MODULE(pomaicache, m) {
  py::class_<PyCache>(m, "Cache")
      .def(py::init<const std::string &, std::uint64_t>(),
           py::arg("data_dir") = "./data",
           py::arg("memory_limit_bytes") = 128 * 1024 * 1024)
      .def("prompt_put", &PyCache::prompt_put,
           py::arg("tokens"),
           py::arg("artifact"),
           py::arg("ttl_ms") = 300000)
      .def("prompt_get", &PyCache::prompt_get,
           py::arg("tokens"));
}

