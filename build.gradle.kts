plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.spetakkel.AppKt"
}

dependencies {
    implementation(libs.rapidsAndRivers)

    implementation(libs.flyway.postgresql)
    implementation(libs.hikariCP)
    implementation(libs.postgresql)
    implementation(libs.kotliquery)

    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
    testImplementation(libs.tbdLibs.postgresTestdatabaser)
}
