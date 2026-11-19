# Multi-architecture index digest for maven:3.9.11-eclipse-temurin-21.
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-26@sha256:b2c1ad85954592f9928e84327c65201f308ad9b5d8ed7d5b823717c97bf23fbb AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests clean package -pl dk.coordinator -am

# Multi-architecture index digest for eclipse-temurin:21-jre-jammy.
FROM eclipse-temurin:21-jre-jammy@sha256:affcab5356e1270ac35d2361ee83135af2f03e8d580eca78a6d5d6fdbf048db0
RUN groupadd --gid 10001 dsearch \
    && useradd --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin dsearch \
    && install --directory --owner=10001 --group=10001 /data
WORKDIR /app
COPY --from=build --chown=10001:10001 /app/dk.coordinator/target/dk-coordinator.jar app.jar

EXPOSE 7000
USER 10001:10001
ENTRYPOINT ["java", "-jar", "app.jar"]
