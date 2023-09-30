import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val junitJupiterVersion = "5.10.0"
val testcontainersVersion = "1.19.0"
val mainClass = "no.nav.helse.spetakkel.AppKt"

plugins {
    kotlin("jvm") version "1.9.10"
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:2023093008351696055717.ffdec6aede3d")

    implementation("org.flywaydb:flyway-core:9.10.2")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.github.seratch:kotliquery:1.9.0")

    implementation("com.bazaarvoice.jackson:rison:2.9.10.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

tasks {
    withType<Jar> {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = mainClass
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists()) it.copyTo(file)
            }
        }
    }

    named<KotlinCompile>("compileKotlin") {
        kotlinOptions.jvmTarget = "17"
    }

    named<KotlinCompile>("compileTestKotlin") {
        kotlinOptions.jvmTarget = "17"
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("skipped", "failed")
        }
    }

    withType<Wrapper> {
        gradleVersion = "8.3"
    }
}
