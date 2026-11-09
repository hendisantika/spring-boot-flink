# spring-boot-flink

[![Java CI with Maven](https://github.com/hendisantika/spring-boot-flink/actions/workflows/maven.yml/badge.svg)](https://github.com/hendisantika/spring-boot-flink/actions/workflows/maven.yml)
![Java](https://img.shields.io/badge/Java-25-orange?logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-6DB33F?logo=springboot)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-2.3-E6526F?logo=apacheflink)
![Dependabot](https://img.shields.io/badge/Dependabot-enabled-025E8C?logo=dependabot)

Sample project integrating **Apache Flink** with **Spring Boot**: word count over files and sockets, a simple
string-processing job launched from a Spring bean, and a local Flink cluster with the Web UI.

## Tech Stack

| Component        | Version                                  |
|------------------|------------------------------------------|
| Java (JDK)       | 25 (Temurin)                             |
| Spring Boot      | 4.1.x                                    |
| Apache Flink     | 2.3.x (`flink-java` 1.20.x for DataSet)  |
| Flink Kafka      | 5.0.0-2.2                                |
| Build            | Maven (wrapper included) + Shade plugin  |

## Project Structure

```
src/main/java/id/my/hendisantika/flink
├── SpringBootFlinkApplication.java   # Spring Boot entry point, runs FlinkJobService on startup
├── service/FlinkJobService.java      # Triggers StringProcessingJob
└── job
    ├── StringProcessingJob.java      # Maps in-memory elements -> "Processed: <value>"
    ├── DataSetBatch.java             # Batch word count (legacy DataSet API) over word.txt
    ├── DataStreamAPI.java            # Streaming word count with DataStream API
    ├── SocketJob.java                # Word count from a socket text stream
    └── FlinkWebUI.java               # Local environment with Flink Web UI on port 7777
src/main/resources
├── application.yml                   # Server & Flink JobManager settings
└── word.txt                          # Sample input for word count
```

## Prerequisites

- JDK 25
- Maven 3.9+ (or use the bundled `./mvnw`)
- Optional: `nc` (netcat) to feed text for the socket jobs

## Build & Test

```bash
./mvnw clean package
```

This produces a shaded (fat) jar at `target/flink-0.0.1-SNAPSHOT.jar`.

## Run

Run the Spring Boot application (starts on port `8080` and executes `StringProcessingJob`):

```bash
./mvnw spring-boot:run
```

Run the socket word count job. Start a socket server first, then type words into it:

```bash
nc -lk 8888
```

> `SocketJob` and `FlinkWebUI` connect to `172.24.4.193:8888` by default — change the host in the source
> (e.g. to `localhost`) to match your environment.

`FlinkWebUI` starts a local Flink cluster with the dashboard at <http://localhost:7777>.

The other jobs (`DataSetBatch`, `DataStreamAPI`, `StringProcessingJob`) can be run directly from your IDE via
their `main` method.

## Configuration

`src/main/resources/application.yml`:

```yaml
spring.application.name: spring-boot-flink
server:
  port: 8080

flink:
  job-manager-host: 172.24.4.193
  job-manager-port: 8082
```

## CI/CD & Dependency Management

- **GitHub Actions** (`.github/workflows/maven.yml`): builds and tests on JDK 25 for every push and pull
  request to `main`, then submits the Maven dependency graph to GitHub on `main`.
- **Dependabot** (`.github/dependabot.yml`):
  - Maven updates daily at 05:00 (Asia/Jakarta), grouped by Spring Boot, Apache Flink, Maven plugins and
    minor/patch updates.
  - GitHub Actions updates weekly in a single grouped PR.
  - `flink-java` major updates are ignored because Flink 2.x removes the DataSet API.

## Author

**Hendi Santika**

- Email: hendisantika@gmail.com
- Telegram: [@hendisantika34](https://t.me/hendisantika34)
- Link: [s.id/hendisantika](https://s.id/hendisantika)
