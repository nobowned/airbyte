/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.core.operation

import io.airbyte.cdk.core.operation.executor.OperationExecutor
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DefaultCheckOperationTest {
    @Test
    internal fun testThatTheCorrectOperationTypeIsReturned() {
        val operationExecutor: OperationExecutor = mockk()
        val operation = DefaultCheckOperation(operationExecutor = operationExecutor)
        assertEquals(OperationType.CHECK, operation.type())
    }

    @Test
    internal fun testThatOnSuccessfulExecutionOfTheOperationTheResultIsReturned() {
        val operationExecutor: OperationExecutor = mockk()

        every { operationExecutor.execute() } returns Result.success(AirbyteMessage())

        val operation = DefaultCheckOperation(operationExecutor = operationExecutor)

        val result = operation.execute()
        assertTrue(result.isSuccess)
        verify { operationExecutor.execute() }
    }

    @Test
    internal fun testThatOnAFailedExecutionOfTheOperationTheFailedCheckMessageIsReturned() {
        val operationExecutor: OperationExecutor = mockk()
        val failure = NullPointerException("test")
        val expectedMessage =
            AirbyteMessage()
                .withType(AirbyteMessage.Type.CONNECTION_STATUS)
                .withConnectionStatus(
                    AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.FAILED)
                        .withMessage(failure.message),
                )

        every { operationExecutor.execute() } returns Result.failure(failure)

        val operation = DefaultCheckOperation(operationExecutor = operationExecutor)

        val result = operation.execute()
        assertTrue(result.isSuccess)
        assertEquals(expectedMessage, result.getOrNull())
        verify { operationExecutor.execute() }
    }

    @Test
    internal fun testRequiresCatalog() {
        val operationExecutor: OperationExecutor = mockk()
        val operation = DefaultCheckOperation(operationExecutor = operationExecutor)
        assertFalse(operation.requiresCatalog())
    }

    @Test
    internal fun testRequiresConfiguration() {
        val operationExecutor: OperationExecutor = mockk()
        val operation = DefaultCheckOperation(operationExecutor = operationExecutor)
        assertTrue(operation.requiresConfiguration())
    }
}
