package net.corda.node.internal

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import com.zaxxer.hikari.pool.HikariPool
import com.zaxxer.hikari.util.PropertyElf
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.declaredField
import net.corda.node.services.api.SchemaService
import net.corda.node.services.persistence.AbstractPartyDescriptor
import net.corda.node.services.persistence.AbstractPartyToX500NameAsStringConverter
import net.corda.node.services.schema.NodeSchemaService
import net.corda.nodeapi.internal.persistence.CordaPersistence
import net.corda.nodeapi.internal.persistence.CouldNotCreateDataSourceException
import net.corda.nodeapi.internal.persistence.DatabaseConfig
import net.corda.nodeapi.internal.persistence.IncompatibleAttachmentsContractsTableName
import org.h2.engine.Database
import org.h2.engine.Engine
import org.hibernate.type.descriptor.java.JavaTypeDescriptorRegistry
import org.slf4j.LoggerFactory
import java.lang.reflect.Modifier
import java.util.*
import javax.sql.DataSource

object DataSourceFactory {
    /** H2 only uses get/put/remove on [Engine.DATABASES], and it has to be a [HashMap]. */
    private class SynchronizedGetPutRemove<K, V> : HashMap<K, V>() {
        @Synchronized
        override fun get(key: K) = super.get(key)

        @Synchronized
        override fun put(key: K, value: V) = super.put(key, value)

        @Synchronized
        override fun remove(key: K) = super.remove(key)
    }

    init {
        LoggerFactory.getLogger(javaClass).debug("Applying H2 fix.") // See CORDA-924.
        Engine::class.java.getDeclaredField("DATABASES").apply {
            isAccessible = true
            declaredField<Int>("modifiers").apply { value = value and Modifier.FINAL.inv() }
        }.set(null, SynchronizedGetPutRemove<String, Database>())
    }

    fun createDataSource(hikariProperties: Properties, pool: Boolean = true): DataSource {
        val config = HikariConfig(hikariProperties)
        return if (pool) {
            HikariDataSource(config)
        } else {
            // Basic init for the one test that wants to go via this API but without starting a HikariPool:
            (Class.forName(hikariProperties.getProperty("dataSourceClassName")).newInstance() as DataSource).also {
                PropertyElf.setTargetFromProperties(it, config.dataSourceProperties)
            }
        }
    }
}

fun createCordaPersistence(databaseConfig: DatabaseConfig,
                           wellKnownPartyFromX500Name: (CordaX500Name) -> Party?,
                           wellKnownPartyFromAnonymous: (AbstractParty) -> Party?,
                           schemaService: SchemaService): CordaPersistence {
    // Register the AbstractPartyDescriptor so Hibernate doesn't warn when encountering AbstractParty. Unfortunately
    // Hibernate warns about not being able to find a descriptor if we don't provide one, but won't use it by default
    // so we end up providing both descriptor and converter. We should re-examine this in later versions to see if
    // either Hibernate can be convinced to stop warning, use the descriptor by default, or something else.
    JavaTypeDescriptorRegistry.INSTANCE.addDescriptor(AbstractPartyDescriptor(wellKnownPartyFromX500Name, wellKnownPartyFromAnonymous))
    val attributeConverters = listOf(AbstractPartyToX500NameAsStringConverter(wellKnownPartyFromX500Name, wellKnownPartyFromAnonymous))
    return CordaPersistence(databaseConfig, schemaService.schemaOptions.keys, attributeConverters)
}

fun configureDatabase(hikariProperties: Properties,
                      databaseConfig: DatabaseConfig,
                      wellKnownPartyFromX500Name: (CordaX500Name) -> Party?,
                      wellKnownPartyFromAnonymous: (AbstractParty) -> Party?,
                      schemaService: SchemaService = NodeSchemaService()): CordaPersistence {
    val persistence = createCordaPersistence(databaseConfig, wellKnownPartyFromX500Name, wellKnownPartyFromAnonymous, schemaService)
    persistence.hikariStart(hikariProperties)
    return persistence
}

fun CordaPersistence.hikariStart(hikariProperties: Properties) {
    try {
        start(DataSourceFactory.createDataSource(hikariProperties))
    } catch (e: Exception) {
        when {
            e is HikariPool.PoolInitializationException -> throw CouldNotCreateDataSourceException("Could not connect to the database. Please check your JDBC connection URL, or the connectivity to the database.", e)
            e.cause is ClassNotFoundException -> throw CouldNotCreateDataSourceException("Could not find the database driver class. Please add it to the 'drivers' folder. See: https://docs.corda.net/corda-configuration-file.html")
            e is IncompatibleAttachmentsContractsTableName -> throw e
            else -> throw CouldNotCreateDataSourceException("Could not create the DataSource: ${e.message}", e)
        }
    }
}
