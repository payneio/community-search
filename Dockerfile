FROM rust:1-bookworm AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r -g 999 appuser && useradd -r -u 999 -g appuser appuser
RUN mkdir -p /data && chown appuser:appuser /data

COPY --from=builder /app/target/release/community-search /usr/local/bin/community-search

USER appuser

ENV COMMUNITY_SEARCH_BIND_ADDR=0.0.0.0
ENV COMMUNITY_SEARCH_PORT=8080
ENV COMMUNITY_SEARCH_DATA_DIR=/data
ENV COMMUNITY_SEARCH_INDEX_PATH=/data/index

EXPOSE 8080

CMD ["community-search"]
