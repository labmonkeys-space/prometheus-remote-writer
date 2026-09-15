# The image ClusterFuzzLite builds the fuzzers in.
#
# The OSS-Fuzz JVM base image ships JDK 17 and this project compiles for 21,
# so a JDK 21 is installed over it. Both the base image and the JDK are
# pinned by digest and checked: an unpinned toolchain download is a
# supply-chain hole in the one place that is meant to be looking for them.
FROM gcr.io/oss-fuzz-base/base-builder-jvm@sha256:cc3c3da52c2f48d7a6caf1d5736ab8ec8cb213f4345329b33cb6e497780b1ed0

ENV JDK_URL=https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12.1%2B1/OpenJDK21U-jdk_x64_linux_hotspot_21.0.12.1_1.tar.gz
ENV JDK_SHA256=ce79869e1307ed8ee1e2baa86a412b1eb5b75d10a01006d788a6f968bcfaee94
ENV JAVA_HOME=/usr/lib/jvm/jdk-21
ENV PATH=/usr/lib/jvm/jdk-21/bin:$PATH

RUN set -eux; \
    curl -fsSL "$JDK_URL" -o /tmp/jdk.tar.gz; \
    echo "$JDK_SHA256  /tmp/jdk.tar.gz" | sha256sum -c -; \
    mkdir -p "$JAVA_HOME"; \
    tar -xzf /tmp/jdk.tar.gz --strip-components=1 -C "$JAVA_HOME"; \
    rm /tmp/jdk.tar.gz; \
    java -version

COPY . $SRC/prometheus-remote-writer
COPY .clusterfuzzlite/build.sh $SRC/
WORKDIR $SRC/prometheus-remote-writer
