import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { z } from 'zod';
import { KafkaService } from '../services/kafka.service';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyMcpServer = { registerTool: (name: string, config: any, cb: any) => void };

export function registerConsumerTools(server: McpServer, kafka: KafkaService): void {
  const s = server as unknown as AnyMcpServer;

  s.registerTool(
    'list_consumer_groups',
    { description: 'List all consumer groups registered on the Kafka cluster' },
    async () => {
      try {
        const groups = await kafka.listConsumerGroups();
        return { content: [{ type: 'text' as const, text: JSON.stringify(groups, null, 2) }] };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${message}` }] };
      }
    },
  );

  s.registerTool(
    'consumer_group_offsets',
    {
      description: 'Query committed offsets for a consumer group',
      inputSchema: {
        groupId: z.string().describe('Consumer group ID'),
        topic: z.string().optional().describe('Filter to a specific topic (omit to query all topics)'),
      },
    },
    async ({ groupId, topic }: { groupId: string; topic?: string }) => {
      try {
        const offsets = await kafka.consumerGroupOffsets(groupId, topic);
        return { content: [{ type: 'text' as const, text: JSON.stringify(offsets, null, 2) }] };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${message}` }] };
      }
    },
  );
}
