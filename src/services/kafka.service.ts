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

export class KafkaService {
  private kafka: Kafka;
  private admin: Admin | null = null;

  constructor(brokers: string[]) {
    this.kafka = new Kafka({ clientId: 'kafka-mcp', brokers });
  }

  private async getAdmin(): Promise<Admin> {
    if (!this.admin) {
      this.admin = this.kafka.admin();
      await this.admin.connect();
    }
    return this.admin;
  }

  async listTopics(): Promise<string[]> {
    const admin = await this.getAdmin();
    return admin.listTopics();
  }

  async topicMetadata(topic: string) {
    const admin = await this.getAdmin();
    const result = await admin.fetchTopicMetadata({ topics: [topic] });
    return result.topics[0];
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
  ): Promise<MessageResult[]> {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    const messages: MessageResult[] = [];
    let done: () => void;
    const collected = new Promise<void>((resolve) => { done = resolve; });
    const timeout = new Promise<void>((resolve) => setTimeout(resolve, 5000));

    consumer.run({
      eachMessage: async ({ message, partition }) => {
        messages.push({
          key: message.key ? message.key.toString() : null,
          value: message.value ? message.value.toString() : null,
          partition,
          offset: message.offset,
          timestamp: message.timestamp,
        });
        if (messages.length >= limit) done!();
      },
    });

    await Promise.race([collected, timeout]);
    await consumer.disconnect();
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
    return admin.fetchOffsets({ groupId, topics }) as Promise<OffsetResult[]>;
  }

  async disconnect(): Promise<void> {
    if (this.admin) {
      await this.admin.disconnect();
      this.admin = null;
    }
  }
}
