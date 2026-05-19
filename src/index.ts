import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio';
import { resolveConfig } from './config';
import { KafkaService } from './services/kafka.service';
import { registerTopicTools } from './tools/topics';
import { registerMessageTools } from './tools/messages';
import { registerConsumerTools } from './tools/consumers';

async function main() {
  const config = resolveConfig();
  const kafka = new KafkaService(config.brokers);

  const server = new McpServer({
    name: 'kafka-mcp',
    version: '1.0.0',
  });

  registerTopicTools(server, kafka);
  registerMessageTools(server, kafka);
  registerConsumerTools(server, kafka);

  let shuttingDown = false;
  const shutdown = async () => {
    if (shuttingDown) return;
    shuttingDown = true;
    try {
      await kafka.disconnect();
    } catch {
      // best-effort
    }
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error('Fatal:', err instanceof Error ? err.message : String(err));
  process.exit(1);
});
