plugins {
    java
    id("io.quarkus") version "3.8.6"
}

repositories {
    mavenCentral()
}

val quarkusPlatformGroupId: String by project
val quarkusPlatformArtifactId: String by project
val quarkusPlatformVersion: String by project

dependencies {
    implementation(enforcedPlatform("${quarkusPlatformGroupId}:${quarkusPlatformArtifactId}:${quarkusPlatformVersion}"))

    implementation("io.quarkus:quarkus-arc")
    implementation("io.quarkus:quarkus-smallrye-reactive-messaging-kafka")
    implementation("io.quarkus:quarkus-jackson")
    implementation("io.quarkus:quarkus-config-yaml")

    testImplementation("io.quarkus:quarkus-junit5")
    testImplementation("io.quarkus:quarkus-test-kafka-companion")
}

group = "com.example"
version = "0.1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<Test> {
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    // Forward CC integration flag to test JVM
    systemProperty("cc.integration", System.getProperty("cc.integration") ?: "false")
    systemProperty("cc.compute-pool", System.getProperty("cc.compute-pool") ?: "lfcp-kknvdm")
    systemProperty("cc.cluster", System.getProperty("cc.cluster") ?: "lkc-6w3rv2")
    systemProperty("cc.environment", System.getProperty("cc.environment") ?: "env-nvv5xz")
    systemProperty("cc.cloud", System.getProperty("cc.cloud") ?: "aws")
    systemProperty("cc.region", System.getProperty("cc.region") ?: "eu-central-1")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}
