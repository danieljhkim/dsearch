# Multi-architecture index digest for maven:3.9.11-eclipse-temurin-21.
FROM --platform=$BUILDPLATFORM maven:3-eclipse-temurin-26@sha256:5bfb178bea156ab53b779b2be338e8bfb45decfed35d83da92f1dd9b7348845c AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp clean package -pl dk.index-node -am

# Multi-architecture index digest for eclipse-temurin:21-jre-jammy.
FROM eclipse-temurin:21-jre-jammy@sha256:bce52ea7da1f72e6bf5bec505e63b6eb55ba79ad1226903579f77eab1a80139a
RUN groupadd --gid 10001 dsearch \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin dsearch \
    && install --directory --owner=10001 --group=10001 /data/index /var/cache/dsearch
WORKDIR /app
COPY --from=build --chown=10001:10001 /app/dk.index-node/target/dk-index-node.jar app.jar

EXPOSE 5000 5100
USER 10001:10001
ENTRYPOINT ["java", "--add-modules", "jdk.incubator.vector", "-jar", "app.jar"]
