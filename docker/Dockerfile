FROM debian:bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j

FROM debian:bookworm-slim AS runtime
RUN useradd -m app
WORKDIR /app
COPY --from=build /src/build/pomai_cache_server /usr/local/bin/pomai_cache_server
COPY config /app/config
EXPOSE 6379 9108
USER app
CMD ["/usr/local/bin/pomai_cache_server","--port","6379","--policy","pomai_cost","--params","/app/config/policy_params.json"]
