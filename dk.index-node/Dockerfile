# Multi-architecture index digest for maven:3.9.11-eclipse-temurin-21.
FROM --platform=$BUILDPLATFORM maven:3.9.15-eclipse-temurin-26@sha256:029a8e2838ae68238ffb8be407cddbb3f07d4d839c60c6f26c619a69fd184531 AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp clean package -pl dk.index-node -am

# Multi-architecture index digest for eclipse-temurin:21-jre-jammy.
FROM eclipse-temurin:21-jre-jammy@sha256:eebd356ad7358b7094758e5787a6726f332917cfd56feab6457c56dab895cdbf
RUN groupadd --gid 10001 dsearch \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin dsearch \
    && install --directory --owner=10001 --group=10001 /data/index /var/cache/dsearch
WORKDIR /app
COPY --from=build --chown=10001:10001 /app/dk.index-node/target/dk-index-node.jar app.jar

EXPOSE 5000 5100
USER 10001:10001
ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-jar", "app.jar"]
