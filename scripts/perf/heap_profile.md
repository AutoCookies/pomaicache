# Heap profiling
Use jemalloc or mimalloc profiling if available.

Example (jemalloc):
```
MALLOC_CONF=prof:true,lg_prof_sample:19,prof_prefix:jeprof ./build/pomai_cache_server
```
