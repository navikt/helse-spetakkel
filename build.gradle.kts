import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val junitJupiterVersion = "5.6.2"
val mainClass = "no.nav.helse.spetakkel.AppKt"

plugins {
    kotlin("jvm") version "1.3.72"
}

buildscript {
    dependencies {
        classpath("org.junit.platform:junit-platform-gradle-plugin:1.2.0")
    }
}

val githubUser: String by project
val githubPassword: String by project

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:fa839faa1c")

    implementation("org.flywaydb:flyway-core:6.5.0")
    implementation("com.zaxxer:HikariCP:3.4.5")
    implementation("no.nav:vault-jdbc:1.3.1")
    implementation("com.github.seratch:kotliquery:1.3.1")

    implementation("com.bazaarvoice.jackson:rison:2.9.10.2")

    testImplementation("com.opentable.components:otj-pg-embedded:0.13.3")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
}

repositories {
    jcenter()
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
        kotlinOptions.jvmTarget = "12"
    }

    named<KotlinCompile>("compileTestKotlin") {
        kotlinOptions.jvmTarget = "12"
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    withType<Wrapper> {
        gradleVersion = "6.5.1"
    }
}
