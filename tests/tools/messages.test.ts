import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { KafkaService } from '../../src/services/kafka.service';
import { registerMessageTools } from '../../src/tools/messages';

jest.mock('../../src/services/kafka.service');

describe('message tools', () => {
  let server: McpServer;
  let kafka: jest.Mocked<KafkaService>;
  const registeredTools: Record<string, (args: any) => Promise<any>> = {};

  beforeEach(() => {
    kafka = new KafkaService([]) as jest.Mocked<KafkaService>;
    server = {
      registerTool: jest.fn((name, _config, handler) => { registeredTools[name] = handler; }),
    } as unknown as McpServer;
    registerMessageTools(server, kafka);
  });

  it('registers produce_message and consume_messages tools', () => {
    expect((server.registerTool as jest.Mock)).toHaveBeenCalledWith('produce_message', expect.any(Object), expect.any(Function));
    expect((server.registerTool as jest.Mock)).toHaveBeenCalledWith('consume_messages', expect.any(Object), expect.any(Function));
  });

  describe('produce_message', () => {
    it('sends message and returns confirmation', async () => {
      kafka.produceMessage.mockResolvedValue({ topicName: 'topic-a', partition: 0, baseOffset: '5' });
      const result = await registeredTools['produce_message']({ topic: 'topic-a', message: 'hello' });
      expect(result.content[0].text).toContain('topic-a');
      expect(kafka.produceMessage).toHaveBeenCalledWith('topic-a', 'hello', undefined, undefined);
    });

    it('passes optional key and partition', async () => {
      kafka.produceMessage.mockResolvedValue({ topicName: 'topic-a', partition: 2, baseOffset: '10' });
      await registeredTools['produce_message']({ topic: 'topic-a', message: 'hello', key: 'my-key', partition: 2 });
      expect(kafka.produceMessage).toHaveBeenCalledWith('topic-a', 'hello', 'my-key', 2);
    });

    it('returns error response on failure', async () => {
      kafka.produceMessage.mockRejectedValue(new Error('send failed'));
      const result = await registeredTools['produce_message']({ topic: 'topic-a', message: 'hello' });
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('send failed');
    });
  });

  describe('consume_messages', () => {
    it('uses default limit of 20 and groupId of tmp', async () => {
      kafka.consumeMessages.mockResolvedValue([]);
      await registeredTools['consume_messages']({ topic: 'topic-a' });
      expect(kafka.consumeMessages).toHaveBeenCalledWith('topic-a', 20, 'tmp');
    });

    it('caps limit at 500', async () => {
      kafka.consumeMessages.mockResolvedValue([]);
      await registeredTools['consume_messages']({ topic: 'topic-a', limit: 9999 });
      expect(kafka.consumeMessages).toHaveBeenCalledWith('topic-a', 500, 'tmp');
    });

    it('uses provided groupId', async () => {
      kafka.consumeMessages.mockResolvedValue([]);
      await registeredTools['consume_messages']({ topic: 'topic-a', groupId: 'my-group' });
      expect(kafka.consumeMessages).toHaveBeenCalledWith('topic-a', 20, 'my-group');
    });

    it('returns messages as JSON', async () => {
      kafka.consumeMessages.mockResolvedValue([
        { key: 'k1', value: 'v1', partition: 0, offset: '0', timestamp: '1000' },
      ]);
      const result = await registeredTools['consume_messages']({ topic: 'topic-a' });
      expect(result.content[0].text).toContain('v1');
    });

    it('returns error response on failure', async () => {
      kafka.consumeMessages.mockRejectedValue(new Error('consume failed'));
      const result = await registeredTools['consume_messages']({ topic: 'topic-a' });
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('consume failed');
    });
  });
});
