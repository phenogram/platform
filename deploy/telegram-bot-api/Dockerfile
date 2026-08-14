FROM debian:bookworm-slim AS builder

ARG TELEGRAM_BOT_API_REF=adfd7f6a8e990272851777eeb3ae0def4216f161
ARG TDLIB_REF=a9966eb3704a3351568c28013fed67d797c17828
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates cmake g++ git gperf libssl-dev make zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY deploy/telegram-bot-api/patches/0001-phenogram-update-tap.patch /patches/0001-phenogram-update-tap.patch
COPY deploy/telegram-bot-api/tests/verify-patched-source.sh /patches/verify-patched-source.sh
RUN git clone --filter=blob:none https://github.com/tdlib/telegram-bot-api.git . \
    && git checkout "${TELEGRAM_BOT_API_REF}" \
    && test "$(git rev-parse HEAD)" = "${TELEGRAM_BOT_API_REF}" \
    && git submodule update --init --recursive --depth 1 \
    && test "$(git -C td rev-parse HEAD)" = "${TDLIB_REF}" \
    && git apply --check /patches/0001-phenogram-update-tap.patch \
    && git apply /patches/0001-phenogram-update-tap.patch \
    && sh /patches/verify-patched-source.sh /source \
    && git diff --check
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/telegram \
       -DPHENOGRAM_UPDATE_TAP_TESTS=ON \
    && cmake --build build --target update-tap-test --parallel 2 \
    && ctest --test-dir build --verbose --tests-regex '^update-tap-test$' \
    && cmake --build build --target install --parallel 2

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10002 telegram \
    && install -d -o telegram -g telegram /var/lib/telegram-bot-api/temp
COPY --from=builder /opt/telegram/bin/telegram-bot-api /usr/local/bin/telegram-bot-api
USER telegram
VOLUME ["/var/lib/telegram-bot-api"]
EXPOSE 8081
ENTRYPOINT ["telegram-bot-api"]
CMD ["--local", "--http-port=8081", "--dir=/var/lib/telegram-bot-api", "--temp-dir=/var/lib/telegram-bot-api/temp"]
