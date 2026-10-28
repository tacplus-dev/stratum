// Copyright (c) 2026 Jody Scott
// SPDX-License-Identifier: Apache-2.0
package com.github.tacplusdev.stratum.hazelcast;

import java.util.List;
import java.util.UUID;

import com.google.protobuf.Message;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.spi.partitiongroup.PartitionGroupMetaData;
import com.hazelcast.topic.TopicOverloadPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tacplusdev.stratum.StratumRecordTypes;
import com.github.tacplusdev.stratum.store.RecordStore;

/**
 * The Hazelcast config of a tenant's record store.
 *
 * <p>Every member is the same: it holds partitions (primaries and backups) of
 * every record map and answers queries over them. Memory is availability, not
 * durability (the redo service is durability, see {@link RecordStore}), so there
 * are no member roles, no volumes, no quorum of durable copies and no idle
 * clock: each tenant's dataset stays resident, and the memory request is sized
 * for it. A single member is allowed; it trades availability (a restart is
 * downtime until the replay finishes), never an acked write.
 */
public final class StratumMapConfig {

    private static final Logger log = LoggerFactory.getLogger(StratumMapConfig.class);

    /** Must match the chart's HasHazelcast containerPort (kernel's convention). */
    public static final int HAZELCAST_PORT = 5001;

    /**
     * Synchronous in-memory backups per map, beyond the primary: how many members
     * may die at once without the cluster losing a row it would then have to
     * replay from the redo.
     */
    public static final int DEFAULT_BACKUP_COUNT = 1;

    /** Env name for the {@link #DEFAULT_BACKUP_COUNT} override. */
    public static final String BACKUP_COUNT_ENV = "STRATUM_BACKUP_COUNT";

    /**
     * Env name for this member's availability zone (the operator sets it from the
     * node's topology.kubernetes.io/zone). When set, backups are zone-aware: a
     * partition's backup is never in its primary's zone, so a zone loss costs no
     * row while another zone holds a member.
     */
    public static final String ZONE_ENV = "STRATUM_ZONE";

    public static final String CLUSTER_NAME = "stratum";

    /** The record query plane's open result sets, shared by every member (see StratumRecordMaps). */
    public static final String RECORD_QUERIES_MAP = "stratum.record-queries";

    /** Threads serving writes forwarded to the sequencer; each waits on a redo ack. */
    static final int SEQUENCER_THREADS = 64;

    /** Committed operations kept for members that read the change topic late. */
    static final int CHANGES_CAPACITY = 65536;

    private StratumMapConfig() {}

    /**
     * How many synchronous backups each map carries, from
     * {@code STRATUM_BACKUP_COUNT} or {@link #DEFAULT_BACKUP_COUNT}. Zero means a
     * member's death loses its partitions until the store is replayed.
     */
    public static int configuredBackupCount() {
        String raw = System.getenv(BACKUP_COUNT_ENV);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_BACKUP_COUNT;
        }
        int value = Integer.parseInt(raw.trim());
        if (value < 0 || value > MapConfig.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException(BACKUP_COUNT_ENV + " must be between 0 and "
                    + MapConfig.MAX_BACKUP_COUNT + ", got " + value);
        }
        return value;
    }

    /** This member's zone from {@code STRATUM_ZONE}, or null. */
    public static String configuredZone() {
        String raw = System.getenv(ZONE_ENV);
        return raw == null || raw.isBlank() ? null : raw.trim();
    }

    /**
     * @param seedMembers "host:port" addresses for TCP/IP join; empty for
     *                    multicast (local development only)
     * @param backupCount synchronous backups per map
     * @param zone        this member's availability zone, or null
     */
    public static Config build(List<String> seedMembers, int backupCount, String zone) {
        Config cfg = new Config();
        // Records travel as (proto full name, proto bytes), never as a Java class name.
        cfg.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Message.class)
                .setImplementation(new ProtoMessageSerializer()));
        cfg.setClusterName(CLUSTER_NAME);
        cfg.setInstanceName("stratum-" + UUID.randomUUID().toString().substring(0, 8));
        cfg.setProperty("hazelcast.phone.home.enabled", "false");

        // The port is pinned and auto-increment disabled so a peer's address is
        // predictable: the operator renders seed addresses from the StatefulSet's
        // stable DNS, which only resolves to a member if every member binds this
        // exact port.
        NetworkConfig net = cfg.getNetworkConfig();
        net.setPort(HAZELCAST_PORT);
        net.setPortAutoIncrement(false);
        if (seedMembers == null || seedMembers.isEmpty()) {
            net.getJoin().getMulticastConfig().setEnabled(true);
            net.getJoin().getTcpIpConfig().setEnabled(false);
        } else {
            net.getJoin().getMulticastConfig().setEnabled(false);
            net.getJoin().getTcpIpConfig().setEnabled(true);
            for (String member : seedMembers) {
                net.getJoin().getTcpIpConfig().addMember(member);
            }
        }

        if (zone != null) {
            cfg.getMemberAttributeConfig().setAttribute(PartitionGroupMetaData.PARTITION_GROUP_ZONE, zone);
            cfg.getPartitionGroupConfig().setEnabled(true)
                    .setGroupType(PartitionGroupConfig.MemberGroupType.ZONE_AWARE);
        }

        for (String name : StratumRecordTypes.mapNames()) {
            cfg.addMapConfig(new MapConfig(name).setBackupCount(backupCount).setAsyncBackupCount(0));
        }
        cfg.addMapConfig(new MapConfig(RecordStore.REVISIONS_MAP).setBackupCount(backupCount));
        cfg.addMapConfig(new MapConfig(RecordStore.META_MAP).setBackupCount(backupCount));
        cfg.addMapConfig(new MapConfig(RECORD_QUERIES_MAP).setBackupCount(backupCount));

        // The change feed: one publisher (the sequencer), every member reading.
        cfg.addRingBufferConfig(new RingbufferConfig(RecordStore.CHANGES_TOPIC)
                .setCapacity(CHANGES_CAPACITY).setBackupCount(backupCount));
        cfg.addReliableTopicConfig(new ReliableTopicConfig(RecordStore.CHANGES_TOPIC)
                .setTopicOverloadPolicy(TopicOverloadPolicy.DISCARD_OLDEST));
        cfg.addExecutorConfig(new ExecutorConfig(RecordStore.SEQUENCER_EXECUTOR, SEQUENCER_THREADS));

        log.info("stratum record maps {}: backupCount={}, zone={}, all resident (no eviction)",
                StratumRecordTypes.mapNames(), backupCount, zone == null ? "unset (backups not zone-aware)" : zone);
        return cfg;
    }
}
