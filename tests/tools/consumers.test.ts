import { McpServer } from '@modelcontextprotocol/sdk/server/mcp';
import { KafkaService } from '../../src/services/kafka.service';
import { registerConsumerTools } from '../../src/tools/consumers';

jest.mock('../../src/services/kafka.service');

describe('consumer group tools', () => {
  let server: McpServer;
  let kafka: jest.Mocked<KafkaService>;
  const registeredTools: Record<string, (args: any) => Promise<any>> = {};

  beforeEach(() => {
    kafka = new KafkaService([]) as jest.Mocked<KafkaService>;
    server = {
      registerTool: jest.fn((name, _config, handler) => { registeredTools[name] = handler; }),
    } as unknown as McpServer;
    registerConsumerTools(server, kafka);
  });

  it('registers list_consumer_groups and consumer_group_offsets tools', () => {
    expect((server.registerTool as jest.Mock)).toHaveBeenCalledWith('list_consumer_groups', expect.any(Object), expect.any(Function));
    expect((server.registerTool as jest.Mock)).toHaveBeenCalledWith('consumer_group_offsets', expect.any(Object), expect.any(Function));
  });

  describe('list_consumer_groups', () => {
    it('returns group IDs as JSON', async () => {
      kafka.listConsumerGroups.mockResolvedValue(['group-1', 'group-2']);
      const result = await registeredTools['list_consumer_groups']({});
      expect(result.content[0].text).toContain('group-1');
      expect(result.isError).toBeUndefined();
    });

    it('returns error response on failure', async () => {
      kafka.listConsumerGroups.mockRejectedValue(new Error('admin error'));
      const result = await registeredTools['list_consumer_groups']({});
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('admin error');
    });
  });

  describe('consumer_group_offsets', () => {
    it('fetches offsets for group with specific topic', async () => {
      kafka.consumerGroupOffsets.mockResolvedValue([
        { topic: 'topic-a', partitions: [{ partition: 0, offset: '10', metadata: null }] },
      ]);
      const result = await registeredTools['consumer_group_offsets']({ groupId: 'group-1', topic: 'topic-a' });
      expect(result.content[0].text).toContain('topic-a');
      expect(result.isError).toBeUndefined();
      expect(kafka.consumerGroupOffsets).toHaveBeenCalledWith('group-1', 'topic-a');
    });

    it('fetches offsets without topic filter', async () => {
      kafka.consumerGroupOffsets.mockResolvedValue([]);
      await registeredTools['consumer_group_offsets']({ groupId: 'group-1' });
      expect(kafka.consumerGroupOffsets).toHaveBeenCalledWith('group-1', undefined);
    });

    it('returns error response on failure', async () => {
      kafka.consumerGroupOffsets.mockRejectedValue(new Error('offset error'));
      const result = await registeredTools['consumer_group_offsets']({ groupId: 'group-1' });
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('offset error');
    });
  });
});
