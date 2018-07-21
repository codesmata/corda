package net.corda.node.services.keys

import net.corda.core.node.services.KeyManagementService
import net.corda.nodeapi.internal.persistence.CordaPersistence
import java.security.KeyPair

interface KeyManagementServiceInternal : KeyManagementService {
    fun start(initialKeyPairs: Set<KeyPair>, database: CordaPersistence)
}
