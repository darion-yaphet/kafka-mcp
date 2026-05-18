export interface Config {
  brokers: string[];
}

export function resolveConfig(): Config {
  const brokerArgIndex = process.argv.indexOf('--broker');
  if (brokerArgIndex !== -1 && process.argv[brokerArgIndex + 1]) {
    const raw = process.argv[brokerArgIndex + 1];
    return { brokers: raw.split(',').map((b) => b.trim()).filter(Boolean) };
  }

  const envBrokers = process.env.KAFKA_BROKERS;
  if (envBrokers) {
    return { brokers: envBrokers.split(',').map((b) => b.trim()).filter(Boolean) };
  }

  throw new Error(
    'Kafka broker address required. Set KAFKA_BROKERS env var or pass --broker <address,address>',
  );
}
