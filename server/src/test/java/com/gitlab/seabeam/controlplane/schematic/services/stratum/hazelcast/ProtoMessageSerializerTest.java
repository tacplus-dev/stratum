// Copyright (c) 2024 Second Star Cavalier, LLC.
package com.gitlab.seabeam.controlplane.schematic.services.stratum.hazelcast;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import org.junit.jupiter.api.Test;

import com.gitlab.seabeam.controlplane.schematic.services.stratum.StratumEventStores;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.hot.RecordPersistTask;
import com.gitlab.seabeam.controlplane.schematic.services.stratum.model.types.vservices.agentsensor.Agentsensor;

/**
 * A record crosses Hazelcast as (protobuf full name, bytes): no Java class name of
 * the generated type is on the wire, and a name that is not a record type is
 * refused rather than resolved.
 */
class ProtoMessageSerializerTest {

    /** The serialization service the stratum Config actually configures. */
    private static InternalSerializationService service() {
        return new DefaultSerializationServiceBuilder()
                .setConfig(StratumMapConfig.build(List.of(), 1).getSerializationConfig())
                .build();
    }

    private static Agentsensor.AgentEvent event() {
        return Agentsensor.AgentEvent.newBuilder().setEventId("e1").setStoreEpochMs(42).build();
    }

    private static String wire(Data data) {
        return new String(data.toByteArray(), StandardCharsets.ISO_8859_1);
    }

    @Test
    void aRecordRoundtripsWithoutAJavaClassName() {
        InternalSerializationService ss = service();
        Data data = ss.toData(event());

        assertEquals(ProtoMessageSerializer.TYPE_ID, data.getType());
        String wire = wire(data);
        assertTrue(wire.contains("agentsensor.AgentEvent"), wire);
        assertFalse(wire.contains("Agentsensor"), "the generated class must not be named:\n" + wire);
        assertFalse(wire.contains("com.gitlab"), wire);
        assertEquals(event(), ss.toObject(data));
    }

    /** The serializer writes getFullName() and reads through the registry: they must agree for every type. */
    @Test
    void everyRecordTypeRoundtripsByItsOwnFullName() {
        InternalSerializationService ss = service();
        for (String name : StratumEventStores.mapNames()) {
            Message defaultInstance = StratumEventStores.defaultInstance(name);
            assertEquals(name, defaultInstance.getDescriptorForType().getFullName());
            assertEquals(defaultInstance, ss.toObject(ss.toData(defaultInstance)));
        }
    }

    @Test
    void aRecordInsideAPersistTaskTravelsByFullName() {
        InternalSerializationService ss = service();
        Data data = ss.toData(new RecordPersistTask("agentsensor.AgentEvent", "e1", event(), null, 42));

        String wire = wire(data);
        assertTrue(wire.contains("agentsensor.AgentEvent"), wire);
        assertFalse(wire.contains("Agentsensor$AgentEvent"), "the record must not be Java-serialized:\n" + wire);
        assertEquals(42, ss.<RecordPersistTask>toObject(data).epoch());
    }

    @Test
    void aMessageThatIsNotARecordTypeIsRefused() {
        InternalSerializationService ss = service();
        Data data = ss.toData(Struct.newBuilder()
                .putFields("k", Value.newBuilder().setStringValue("v").build()).build());

        HazelcastSerializationException refused =
                assertThrows(HazelcastSerializationException.class, () -> ss.toObject(data));
        assertTrue(String.valueOf(refused.getMessage()).contains("google.protobuf.Struct")
                        || String.valueOf(refused.getCause()).contains("google.protobuf.Struct"),
                String.valueOf(refused));
    }
}
