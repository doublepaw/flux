// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

package io.flux.sdk;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.flux.sdk.proto.AppendRequest;
import io.flux.sdk.proto.AppendResponse;
import io.flux.sdk.proto.BatchAck;
import io.flux.sdk.proto.ClientMessage;
import io.flux.sdk.proto.JoinGroupRequest;
import io.flux.sdk.proto.Record;
import io.flux.sdk.proto.RecordBatch;
import io.flux.sdk.proto.ServerMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolCompatibilityTest {
    private static final String APPEND_ENVELOPE_HEX =
            "0a2a0a10000102030405060708090a0b0c0d0e0f10071a14080910641a080a026b31120276311a0412027632";
    private static final String APPEND_RESPONSE_ENVELOPE_HEX =
            "0a10080710012a0a0809106418f40320f603";
    private static final String JOIN_GROUP_ENVELOPE_HEX =
            "1a1f0a0c6f72646572732d67726f7570120a636f6e73756d65722d311a03010203";

    @Test
    void testAppendEnvelopeWireVectorIsStable() {
        byte[] writerId = new byte[16];
        for (int i = 0; i < writerId.length; i++) {
            writerId[i] = (byte) i;
        }

        ClientMessage msg = ClientMessage.newBuilder()
                .setAppend(
                        AppendRequest.newBuilder()
                                .setWriterId(ByteString.copyFrom(writerId))
                                .setAppendSeq(7)
                                .addBatches(
                                        RecordBatch.newBuilder()
                                                .setTopicId(9)
                                                .setSchemaId(100)
                                                .addRecords(
                                                        Record.newBuilder()
                                                                .setKey(ByteString.copyFromUtf8("k1"))
                                                                .setValue(ByteString.copyFromUtf8("v1"))
                                                                .build()
                                                )
                                                .addRecords(
                                                        Record.newBuilder()
                                                                .setValue(ByteString.copyFromUtf8("v2"))
                                                                .build()
                                                )
                                                .build()
                                )
                                .build()
                )
                .build();

        assertEquals(APPEND_ENVELOPE_HEX, toHex(msg.toByteArray()));
    }

    @Test
    void testAppendEnvelopeWireVectorDecodes() throws Exception {
        ClientMessage msg = ClientMessage.parseFrom(fromHex(APPEND_ENVELOPE_HEX));
        assertEquals(ClientMessage.MessageCase.APPEND, msg.getMessageCase());

        AppendRequest append = msg.getAppend();
        assertArrayEquals(rangeBytes(16), append.getWriterId().toByteArray());
        assertEquals(7, append.getAppendSeq());
        assertEquals(1, append.getBatchesCount());

        RecordBatch batch = append.getBatches(0);
        assertEquals(2, batch.getRecordsCount());
        assertTrue(batch.getRecords(0).hasKey());
        assertFalse(batch.getRecords(1).hasKey());
    }

    @Test
    void testAppendResponseWireVectorIsStable() {
        ServerMessage msg = ServerMessage.newBuilder()
                .setAppend(
                        AppendResponse.newBuilder()
                                .setAppendSeq(7)
                                .setSuccess(true)
                                .addAppendAcks(
                                        BatchAck.newBuilder()
                                                .setTopicId(9)
                                                .setSchemaId(100)
                                                .setStartOffset(500)
                                                .setEndOffset(502)
                                                .build()
                                )
                                .build()
                )
                .build();

        assertEquals(APPEND_RESPONSE_ENVELOPE_HEX, toHex(msg.toByteArray()));
    }

    @Test
    void testJoinGroupWireVectorIsStable() throws Exception {
        ClientMessage msg = ClientMessage.newBuilder()
                .setJoinGroup(
                        JoinGroupRequest.newBuilder()
                                .setGroupId("orders-group")
                                .setReaderId("consumer-1")
                                .addTopicIds(1)
                                .addTopicIds(2)
                                .addTopicIds(3)
                                .build()
                )
                .build();

        assertEquals(JOIN_GROUP_ENVELOPE_HEX, toHex(msg.toByteArray()));

        ClientMessage decoded = ClientMessage.parseFrom(fromHex(JOIN_GROUP_ENVELOPE_HEX));
        assertEquals(ClientMessage.MessageCase.JOIN_GROUP, decoded.getMessageCase());
        assertEquals("orders-group", decoded.getJoinGroup().getGroupId());
        assertEquals("consumer-1", decoded.getJoinGroup().getReaderId());
        assertEquals(3, decoded.getJoinGroup().getTopicIdsCount());
    }

    @Test
    void testMalformedPayloadRaisesDecodeError() {
        byte[] truncated = fromHex("0a2c0a10000102");
        assertThrows(InvalidProtocolBufferException.class, () -> ClientMessage.parseFrom(truncated));
    }

    private static byte[] fromHex(String hex) {
        int len = hex.length();
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            out[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
        }
        return out;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static byte[] rangeBytes(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}