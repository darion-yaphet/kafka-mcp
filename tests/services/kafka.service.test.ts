import { KafkaService } from '../../src/services/kafka.service';

const mockListTopics = jest.fn().mockResolvedValue(['topic-a', 'topic-b']);
const mockFetchTopicMetadata = jest.fn().mockResolvedValue({
  topics: [{
    name: 'topic-a',
    partitions: [{ partitionId: 0, leader: 1, replicas: [1, 2], isr: [1, 2] }],
  }],
});
const mockListGroups = jest.fn().mockResolvedValue({
  groups: [
    { groupId: 'group-1', protocolType: 'consumer' },
    { groupId: 'group-2', protocolType: 'consumer' },
  ],
});
const mockFetchOffsets = jest.fn().mockResolvedValue([
  { topic: 'topic-a', partitions: [{ partition: 0, offset: '10', metadata: null }] },
]);
const mockAdminConnect = jest.fn().mockResolvedValue(undefined);
const mockAdminDisconnect = jest.fn().mockResolvedValue(undefined);

const mockSend = jest.fn().mockResolvedValue([{
  topicName: 'topic-a',
  partition: 0,
  baseOffset: '5',
}]);
const mockProducerConnect = jest.fn().mockResolvedValue(undefined);
const mockProducerDisconnect = jest.fn().mockResolvedValue(undefined);

const mockConsumerConnect = jest.fn().mockResolvedValue(undefined);
const mockConsumerDisconnect = jest.fn().mockResolvedValue(undefined);
const mockSubscribe = jest.fn().mockResolvedValue(undefined);
const mockRun = jest.fn();

jest.mock('kafkajs', () => ({
  Kafka: jest.fn().mockImplementation(() => ({
    admin: jest.fn().mockReturnValue({
      connect: mockAdminConnect,
      disconnect: mockAdminDisconnect,
      listTopics: mockListTopics,
      fetchTopicMetadata: mockFetchTopicMetadata,
      listGroups: mockListGroups,
      fetchOffsets: mockFetchOffsets,
    }),
    producer: jest.fn().mockReturnValue({
      connect: mockProducerConnect,
      disconnect: mockProducerDisconnect,
      send: mockSend,
    }),
    consumer: jest.fn().mockReturnValue({
      connect: mockConsumerConnect,
      disconnect: mockConsumerDisconnect,
      subscribe: mockSubscribe,
      run: mockRun,
    }),
  })),
}));

describe('KafkaService', () => {
  let service: KafkaService;

  beforeEach(() => {
    jest.clearAllMocks();
    service = new KafkaService(['localhost:9092']);
  });

  afterEach(async () => {
    await service.disconnect();
  });

  describe('listTopics', () => {
    it('returns topic names from admin client', async () => {
      const topics = await service.listTopics();
      expect(topics).toEqual(['topic-a', 'topic-b']);
      expect(mockAdminConnect).toHaveBeenCalledTimes(1);
    });

    it('reuses admin connection on second call', async () => {
      await service.listTopics();
      await service.listTopics();
      expect(mockAdminConnect).toHaveBeenCalledTimes(1);
    });
  });

  describe('topicMetadata', () => {
    it('returns metadata for a topic', async () => {
      const metadata = await service.topicMetadata('topic-a');
      expect(metadata.name).toBe('topic-a');
      expect(metadata.partitions).toHaveLength(1);
      expect(mockFetchTopicMetadata).toHaveBeenCalledWith({ topics: ['topic-a'] });
    });
  });

  describe('produceMessage', () => {
    it('sends a message and returns offset info', async () => {
      const result = await service.produceMessage('topic-a', 'hello');
      expect(result.topicName).toBe('topic-a');
      expect(result.baseOffset).toBe('5');
      expect(mockSend).toHaveBeenCalledWith({
        topic: 'topic-a',
        messages: [{ key: undefined, value: 'hello', partition: undefined }],
      });
    });

    it('passes key and partition when provided', async () => {
      await service.produceMessage('topic-a', 'hello', 'my-key', 2);
      expect(mockSend).toHaveBeenCalledWith({
        topic: 'topic-a',
        messages: [{ key: 'my-key', value: 'hello', partition: 2 }],
      });
    });

    it('disconnects producer after sending', async () => {
      await service.produceMessage('topic-a', 'hello');
      expect(mockProducerDisconnect).toHaveBeenCalledTimes(1);
    });

    it('disconnects producer even when send throws', async () => {
      mockSend.mockRejectedValueOnce(new Error('broker down'));
      await expect(service.produceMessage('topic-a', 'hello')).rejects.toThrow('broker down');
      expect(mockProducerDisconnect).toHaveBeenCalledTimes(1);
    });
  });

  describe('consumeMessages', () => {
    beforeEach(() => jest.useFakeTimers());
    afterEach(() => jest.useRealTimers());

    it('collects messages up to limit and disconnects', async () => {
      mockRun.mockImplementation(async ({ eachMessage }: { eachMessage: Function }) => {
        await eachMessage({
          topic: 'topic-a', partition: 0,
          message: { key: Buffer.from('k1'), value: Buffer.from('v1'), offset: '0', timestamp: '1000' },
        });
        await eachMessage({
          topic: 'topic-a', partition: 0,
          message: { key: null, value: Buffer.from('v2'), offset: '1', timestamp: '1001' },
        });
      });

      const messages = await service.consumeMessages('topic-a', 2, 'test-group');
      expect(messages).toHaveLength(2);
      expect(messages[0]).toEqual({ key: 'k1', value: 'v1', partition: 0, offset: '0', timestamp: '1000' });
      expect(messages[1]).toEqual({ key: null, value: 'v2', partition: 0, offset: '1', timestamp: '1001' });
      expect(mockConsumerDisconnect).toHaveBeenCalledTimes(1);
      expect(mockSubscribe).toHaveBeenCalledWith({ topic: 'topic-a', fromBeginning: true });
    });

    it('returns partial messages on timeout', async () => {
      mockRun.mockImplementation(() => {}); // never fires eachMessage
      const promise = service.consumeMessages('topic-a', 10, 'test-group', 5000);
      // Allow async setup (connect, subscribe) to complete before advancing timers
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
      jest.advanceTimersByTime(5000);
      const messages = await promise;
      expect(messages).toHaveLength(0);
      expect(mockConsumerDisconnect).toHaveBeenCalledTimes(1);
    });
  });

  describe('listConsumerGroups', () => {
    it('returns group IDs', async () => {
      const groups = await service.listConsumerGroups();
      expect(groups).toEqual(['group-1', 'group-2']);
    });
  });

  describe('consumerGroupOffsets', () => {
    it('fetches offsets for a specific topic', async () => {
      const offsets = await service.consumerGroupOffsets('group-1', 'topic-a');
      expect(mockFetchOffsets).toHaveBeenCalledWith({ groupId: 'group-1', topics: ['topic-a'] });
      expect(offsets[0].topic).toBe('topic-a');
    });

    it('fetches offsets for all topics when topic not specified', async () => {
      await service.consumerGroupOffsets('group-1');
      expect(mockListTopics).toHaveBeenCalled();
      expect(mockFetchOffsets).toHaveBeenCalledWith({
        groupId: 'group-1',
        topics: ['topic-a', 'topic-b'],
      });
    });
  });

  describe('disconnect', () => {
    it('disconnects admin client if connected', async () => {
      await service.listTopics();
      await service.disconnect();
      expect(mockAdminDisconnect).toHaveBeenCalledTimes(1);
    });

    it('does nothing if admin was never connected', async () => {
      await service.disconnect();
      expect(mockAdminDisconnect).not.toHaveBeenCalled();
    });
  });
});
