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

  const shutdown = async () => {
    await kafka.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
