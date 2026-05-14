plugins {
    java
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "dev.hialt.foxwatch"
version = "1.0.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

val flinkVersion = "1.19.3"
val kafkaConnectorVersion = "3.3.0-1.19"

dependencies {
    // Flink core — provided at runtime by the Flink cluster
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-clients:$flinkVersion")

    // Kafka connector — bundled into the fat JAR
    implementation("org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    implementation("org.apache.flink:flink-json:$flinkVersion")

    // Jackson for JSON parsing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")




    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

application {
    mainClass.set("dev.hialt.foxwatch.analytics.DropoutDetectorJob")
}

tasks.shadowJar {
    archiveClassifier.set("")
    mergeServiceFiles()
    manifest {
        attributes["Main-Class"] = "dev.hialt.foxwatch.analytics.DropoutDetectorJob"
    }
}

tasks.test {
    useJUnitPlatform()
}