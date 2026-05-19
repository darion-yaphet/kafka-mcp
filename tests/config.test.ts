import { resolveConfig } from '../src/config';

describe('resolveConfig', () => {
  const originalArgv = process.argv;
  const originalEnv = process.env;

  beforeEach(() => {
    process.argv = ['node', 'index.js'];
    process.env = { ...originalEnv };
    delete process.env.KAFKA_BROKERS;
  });

  afterEach(() => {
    process.argv = originalArgv;
    process.env = originalEnv;
  });

  it('uses --broker CLI arg when provided', () => {
    process.argv = ['node', 'index.js', '--broker', 'broker1:9092,broker2:9092'];
    const config = resolveConfig();
    expect(config.brokers).toEqual(['broker1:9092', 'broker2:9092']);
  });

  it('uses KAFKA_BROKERS env var when no CLI arg', () => {
    process.env.KAFKA_BROKERS = 'env-broker:9092';
    const config = resolveConfig();
    expect(config.brokers).toEqual(['env-broker:9092']);
  });

  it('CLI arg takes precedence over env var', () => {
    process.argv = ['node', 'index.js', '--broker', 'cli-broker:9092'];
    process.env.KAFKA_BROKERS = 'env-broker:9092';
    const config = resolveConfig();
    expect(config.brokers).toEqual(['cli-broker:9092']);
  });

  it('throws when neither CLI arg nor env var is set', () => {
    expect(() => resolveConfig()).toThrow('Kafka broker address required');
  });

  it('throws when --broker flag has no value', () => {
    process.argv = ['node', 'index.js', '--broker'];
    expect(() => resolveConfig()).toThrow('--broker flag requires a value');
  });

  it('throws when --broker value produces no valid addresses', () => {
    process.argv = ['node', 'index.js', '--broker', ','];
    expect(() => resolveConfig()).toThrow('no valid addresses');
  });

  it('trims whitespace from broker addresses', () => {
    process.env.KAFKA_BROKERS = ' broker1:9092 , broker2:9092 ';
    const config = resolveConfig();
    expect(config.brokers).toEqual(['broker1:9092', 'broker2:9092']);
  });
});
