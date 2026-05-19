import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { z } from 'zod';
import { KafkaService } from '../services/kafka.service';

export function registerTopicTools(server: McpServer, kafka: KafkaService): void {
  server.tool(
    'list_topics',
    'List all topics on the Kafka cluster',
    {},
    async () => {
      try {
        const topics = await kafka.listTopics();
        return { content: [{ type: 'text' as const, text: JSON.stringify(topics, null, 2) }] };
      } catch (err) {
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${(err as Error).message}` }] };
      }
    },
  );

  const topicSchema = { topic: z.string().describe('Topic name') };

  // @ts-ignore TS2589
  server.tool(
    'topic_metadata',
    'Get metadata for a Kafka topic (partitions, replicas, leader, ISR)',
    topicSchema,
    // @ts-ignore TS2589
    async ({ topic }: { topic: string }) => {
      try {
        const metadata = await kafka.topicMetadata(topic);
        return { content: [{ type: 'text' as const, text: JSON.stringify(metadata, null, 2) }] };
      } catch (err) {
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${(err as Error).message}` }] };
      }
    },
  );
}
