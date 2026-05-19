import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { z } from 'zod';
import { KafkaService } from '../services/kafka.service';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyMcpServer = { registerTool: (name: string, config: any, cb: any) => void };

export function registerTopicTools(server: McpServer, kafka: KafkaService): void {
  const s = server as unknown as AnyMcpServer;

  s.registerTool(
    'list_topics',
    { description: 'List all topics on the Kafka cluster' },
    async () => {
      try {
        const topics = await kafka.listTopics();
        return { content: [{ type: 'text' as const, text: JSON.stringify(topics, null, 2) }] };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${message}` }] };
      }
    },
  );

  s.registerTool(
    'topic_metadata',
    {
      description: 'Get metadata for a Kafka topic (partitions, replicas, leader, ISR)',
      inputSchema: { topic: z.string().describe('Topic name') },
    },
    async ({ topic }: { topic: string }) => {
      try {
        const metadata = await kafka.topicMetadata(topic);
        return { content: [{ type: 'text' as const, text: JSON.stringify(metadata, null, 2) }] };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${message}` }] };
      }
    },
  );
}
