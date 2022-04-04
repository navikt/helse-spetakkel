import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val junitJupiterVersion = "5.8.2"
val testcontainersVersion = "1.16.3"
val mainClass = "no.nav.helse.spetakkel.AppKt"

plugins {
    kotlin("jvm") version "1.6.0"
}

val githubUser: String by project
val githubPassword: String by project

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:2022.04.04-22.16.0611abb2a604")

    implementation("org.flywaydb:flyway-core:8.5.5")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("no.nav:vault-jdbc:1.3.9")
    implementation("com.github.seratch:kotliquery:1.7.0")

    implementation("com.bazaarvoice.jackson:rison:2.9.10.2")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")

    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
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
                val file = File("$buildDir/libs/${it.name}")
                if (!file.exists())
                    it.copyTo(file)
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
        gradleVersion = "7.4.1"
    }
}
