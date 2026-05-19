export interface Config {
  brokers: string[];
}

export function resolveConfig(): Config {
  const brokerArgIndex = process.argv.indexOf('--broker');
  if (brokerArgIndex !== -1) {
    const raw = process.argv[brokerArgIndex + 1];
    if (!raw || raw.startsWith('--')) {
      throw new Error('--broker flag requires a value (e.g., --broker localhost:9092)');
    }
    const brokers = raw.split(',').map((b) => b.trim()).filter(Boolean);
    if (brokers.length === 0) {
      throw new Error('--broker value produced no valid addresses');
    }
    return { brokers };
  }

  const envBrokers = process.env.KAFKA_BROKERS;
  if (envBrokers) {
    const brokers = envBrokers.split(',').map((b) => b.trim()).filter(Boolean);
    if (brokers.length === 0) {
      throw new Error('KAFKA_BROKERS produced no valid addresses');
    }
    return { brokers };
  }

  throw new Error(
    'Kafka broker address required. Set KAFKA_BROKERS env var or pass --broker <address,address>',
  );
}
