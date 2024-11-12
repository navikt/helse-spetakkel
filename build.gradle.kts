val junitJupiterVersion = "5.11.3"
val tbdLibsVersion = "2024.11.12-20.46-e1fd6a25"
val rapidsAndRiversVersion = "2024111220531731441232.6f0a7a6c643b"
val mainClass = "no.nav.helse.spetakkel.AppKt"

plugins {
    kotlin("jvm") version "2.0.21"
}

dependencies {
    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")

    implementation("org.flywaydb:flyway-core:9.10.2")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.postgresql:postgresql:42.7.2")
    implementation("com.github.seratch:kotliquery:1.9.0")

    implementation("com.bazaarvoice.jackson:rison:2.9.10.2")

    testImplementation("com.github.navikt.tbd-libs:rapids-and-rivers-test:$tbdLibsVersion")
    testImplementation("com.github.navikt.tbd-libs:postgres-testdatabaser:$tbdLibsVersion")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

repositories {
    val githubPassword: String? by project
    mavenCentral()
    /* ihht. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
        så plasseres github-maven-repo (med autentisering) før nav-mirror slik at github actions kan anvende førstnevnte.
        Det er fordi nav-mirroret kjører i Google Cloud og da ville man ellers fått unødvendige utgifter til datatrafikk mellom Google Cloud og GitHub
     */
    maven {
        url = uri("https://maven.pkg.github.com/navikt/maven-release")
        credentials {
            username = "x-access-token"
            password = githubPassword
        }
    }
    maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("21"))
    }
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

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("skipped", "failed")
        }
    }
}
