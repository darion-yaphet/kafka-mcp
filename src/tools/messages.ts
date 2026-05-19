import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { z } from 'zod';
import { KafkaService } from '../services/kafka.service';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyMcpServer = { registerTool: (name: string, config: any, cb: any) => void };

const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 20;
const DEFAULT_GROUP_ID = 'tmp';

export function registerMessageTools(server: McpServer, kafka: KafkaService): void {
  const s = server as unknown as AnyMcpServer;

  s.registerTool(
    'produce_message',
    {
      description: 'Write a message to a Kafka topic',
      inputSchema: {
        topic: z.string().describe('Target topic name'),
        message: z.string().describe('Message value to send'),
        key: z.string().optional().describe('Optional message key'),
        partition: z.number().int().optional().describe('Optional target partition number'),
      },
    },
    async ({ topic, message, key, partition }: { topic: string; message: string; key?: string; partition?: number }) => {
      try {
        const result = await kafka.produceMessage(topic, message, key, partition);
        return { content: [{ type: 'text' as const, text: JSON.stringify(result, null, 2) }] };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${msg}` }] };
      }
    },
  );

  s.registerTool(
    'consume_messages',
    {
      description: 'Read messages from a Kafka topic starting from the beginning',
      inputSchema: {
        topic: z.string().describe('Topic name to read from'),
        limit: z.number().int().optional().describe(`Max messages to return (default: ${DEFAULT_LIMIT}, max: ${MAX_LIMIT})`),
        groupId: z.string().optional().describe('Consumer group ID (default: "tmp")'),
      },
    },
    async ({ topic, limit, groupId }: { topic: string; limit?: number; groupId?: string }) => {
      const resolvedLimit = Math.min(limit ?? DEFAULT_LIMIT, MAX_LIMIT);
      const resolvedGroupId = groupId ?? DEFAULT_GROUP_ID;
      try {
        const messages = await kafka.consumeMessages(topic, resolvedLimit, resolvedGroupId);
        return { content: [{ type: 'text' as const, text: JSON.stringify(messages, null, 2) }] };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        return { isError: true, content: [{ type: 'text' as const, text: `Error: ${msg}` }] };
      }
    },
  );
}
