import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { KafkaService } from '../../src/services/kafka.service';
import { registerTopicTools } from '../../src/tools/topics';

jest.mock('../../src/services/kafka.service');

describe('topic tools', () => {
  let server: McpServer;
  let kafka: jest.Mocked<KafkaService>;
  const registeredTools: Record<string, (args: any) => Promise<any>> = {};

  beforeEach(() => {
    kafka = new KafkaService([]) as jest.Mocked<KafkaService>;
    server = {
      tool: jest.fn((name, _desc, _schema, handler) => { registeredTools[name] = handler; }),
    } as unknown as McpServer;
    registerTopicTools(server, kafka);
  });

  describe('list_topics', () => {
    it('returns topic list as JSON', async () => {
      kafka.listTopics.mockResolvedValue(['topic-a', 'topic-b']);
      const result = await registeredTools['list_topics']({});
      expect(result.content[0].text).toContain('topic-a');
      expect(result.isError).toBeUndefined();
    });

    it('returns error response when kafka fails', async () => {
      kafka.listTopics.mockRejectedValue(new Error('broker down'));
      const result = await registeredTools['list_topics']({});
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('broker down');
    });
  });

  describe('topic_metadata', () => {
    it('returns metadata as JSON', async () => {
      kafka.topicMetadata.mockResolvedValue({
        name: 'topic-a',
        partitions: [{ partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
      } as any);
      const result = await registeredTools['topic_metadata']({ topic: 'topic-a' });
      expect(result.content[0].text).toContain('topic-a');
      expect(kafka.topicMetadata).toHaveBeenCalledWith('topic-a');
    });

    it('returns error response when topic not found', async () => {
      kafka.topicMetadata.mockRejectedValue(new Error('topic not found'));
      const result = await registeredTools['topic_metadata']({ topic: 'missing' });
      expect(result.isError).toBe(true);
    });
  });
});
