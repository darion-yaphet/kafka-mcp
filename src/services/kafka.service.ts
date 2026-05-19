import { Kafka, Admin } from 'kafkajs';

export interface MessageResult {
  key: string | null;
  value: string | null;
  partition: number;
  offset: string;
  timestamp: string;
}

export interface ProduceResult {
  topicName: string;
  partition: number;
  baseOffset: string;
}

export interface OffsetResult {
  topic: string;
  partitions: Array<{ partition: number; offset: string; metadata: string | null }>;
}

export interface TopicPartitionMetadata {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface TopicMetadata {
  name: string;
  partitions: TopicPartitionMetadata[];
}

export class KafkaService {
  private kafka: Kafka;
  private admin: Admin | null = null;
  private adminReady: Promise<Admin> | null = null;

  constructor(brokers: string[]) {
    this.kafka = new Kafka({ clientId: 'kafka-mcp', brokers });
  }

  private getAdmin(): Promise<Admin> {
    if (!this.adminReady) {
      this.adminReady = (async () => {
        const admin = this.kafka.admin();
        await admin.connect();
        this.admin = admin;
        return admin;
      })();
    }
    return this.adminReady;
  }

  async listTopics(): Promise<string[]> {
    const admin = await this.getAdmin();
    return admin.listTopics();
  }

  async topicMetadata(topic: string): Promise<TopicMetadata> {
    const admin = await this.getAdmin();
    const result = await admin.fetchTopicMetadata({ topics: [topic] });
    const metadata = result.topics[0];
    if (!metadata) throw new Error(`Topic '${topic}' not found`);
    return metadata as TopicMetadata;
  }

  async produceMessage(
    topic: string,
    message: string,
    key?: string,
    partition?: number,
  ): Promise<ProduceResult> {
    const producer = this.kafka.producer();
    await producer.connect();
    try {
      const results = await producer.send({
        topic,
        messages: [{ key, value: message, partition }],
      });
      const r = results[0];
      return { topicName: r.topicName, partition: r.partition, baseOffset: r.baseOffset ?? '0' };
    } finally {
      await producer.disconnect();
    }
  }

  async consumeMessages(
    topic: string,
    limit: number,
    groupId: string,
    timeoutMs = 5000,
  ): Promise<MessageResult[]> {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();

    const messages: MessageResult[] = [];
    let stop = false;
    let resolveCollected!: () => void;
    const collected = new Promise<void>((r) => { resolveCollected = r; });
    const timeout = new Promise<void>((r) => setTimeout(r, timeoutMs));

    try {
      await consumer.subscribe({ topic, fromBeginning: true });

      let runError: Error | undefined;
      consumer.run({
        eachMessage: async ({ message, partition }) => {
          if (stop) return;
          messages.push({
            key: message.key ? message.key.toString() : null,
            value: message.value ? message.value.toString() : null,
            partition,
            offset: message.offset,
            timestamp: message.timestamp,
          });
          if (messages.length >= limit) resolveCollected();
        },
      }).catch((e) => {
        runError = e instanceof Error ? e : new Error(String(e));
        resolveCollected();
      });

      await Promise.race([collected, timeout]);
      stop = true;

      if (runError) throw runError;
    } finally {
      await consumer.disconnect();
    }

    return messages;
  }

  async listConsumerGroups(): Promise<string[]> {
    const admin = await this.getAdmin();
    const result = await admin.listGroups();
    return result.groups.map((g) => g.groupId);
  }

  async consumerGroupOffsets(groupId: string, topic?: string): Promise<OffsetResult[]> {
    const admin = await this.getAdmin();
    const topics = topic ? [topic] : await admin.listTopics();
    const raw = await admin.fetchOffsets({ groupId, topics });
    return raw.map((entry) => ({
      topic: entry.topic,
      partitions: entry.partitions.map((p) => ({
        partition: p.partition,
        offset: p.offset,
        metadata: p.metadata,
      })),
    }));
  }

  async disconnect(): Promise<void> {
    if (this.admin) {
      await this.admin.disconnect();
      this.admin = null;
      this.adminReady = null;
    }
  }
}
