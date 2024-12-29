import {
  connect,
  StringCodec,
  JetStreamClient,
  NatsConnection,
  JetStreamManager,
  StreamConfig,
  headers,
  jwtAuthenticator,
  RetentionPolicy,
  StorageType,
  AckPolicy,
} from "nats";

const { USER_JWT, USER_SEED } = process.env;

class NatsDelayedQueue {
  private nc: NatsConnection;
  private js: JetStreamClient;
  private jsm: JetStreamManager;
  private sc = StringCodec();
  private readonly streamName = "DELAYED_MESSAGES";
  private readonly subject = "delayed.messages";

  constructor(private readonly servers = ["nats://localhost:4222"]) {}

  async connect(): Promise<void> {
    this.nc = await connect({
      servers: this.servers,
      authenticator: jwtAuthenticator(USER_JWT!, this.sc.encode(USER_SEED!)),
    });
    this.js = this.nc.jetstream();
    this.jsm = await this.nc.jetstreamManager();
    await this.setupStream();
  }

  private async setupStream(): Promise<void> {
    const config: Partial<StreamConfig> = {
      name: this.streamName,
      subjects: [this.subject],
      retention: RetentionPolicy.Workqueue,
      storage: StorageType.Memory,
    };

    try {
      await this.jsm.streams.info(this.streamName);
    } catch {
      await this.jsm.streams.add(config);
      await this.jsm.consumers.add(this.streamName, {
        ack_policy: AckPolicy.Explicit,
        durable_name: "timer_consumer",
      });
    }
  }

  async sendDelayedMessage(
    message: string,
    delaySeconds: number
  ): Promise<void> {
    const deliveryTime = Date.now() + delaySeconds * 1000;
    const hdrs = headers();
    hdrs.append("Delivery-Time", deliveryTime.toString());

    await this.js.publish(this.subject, this.sc.encode(message), {
      headers: hdrs,
    });
  }

  async receiveMessages(
    handler: (message: string) => Promise<void>
  ): Promise<void> {
    console.log(1);
    const consumer = await this.nc
      .jetstream()
      .consumers.get(this.streamName, "timer_consumer");

    console.log(2);

    const messages = await consumer.consume({});

    for await (const msg of messages) {
      const currentTime = Date.now();
      const deliveryTime = parseInt(
        msg.headers?.get("Delivery-Time") || "0",
        10
      );

      if (currentTime >= deliveryTime) {
        try {
          const messageContent = this.sc.decode(msg.data);
          await handler(messageContent);
          msg.ack();

        } catch (error) {
          console.error("Error processing message:", error);
          msg.nak();
        }
      } else {
        const delay = deliveryTime - currentTime;
        // Requeue with remaining delay
        msg.nak(delay);
      }
    }
  }

  async disconnect(): Promise<void> {
    await this.nc?.drain();
    await this.nc?.close();
  }
}

// Example usage
async function main() {
  const queue = new NatsDelayedQueue();

  try {
    await queue.connect();
    console.log("Connected to NATS");

    // Set up message handler
    queue.receiveMessages(async (message) => {
      console.log(
        `Processing message at ${new Date().toISOString()}: ${message}`
      );
    });

    // Send test messages with different delays
    console.log("Sending messages...");
    await queue.sendDelayedMessage("Process in 5 seconds", 5);
    await queue.sendDelayedMessage("Process in 10 seconds", 10);
    await queue.sendDelayedMessage("Process in 15 seconds", 15);

    // Keep running for demo
    await new Promise((resolve) => setTimeout(resolve, 20000));
    await queue.disconnect();
  } catch (error) {
    console.error("Error:", error);
    await queue.disconnect();
  }
}

main().catch(console.error);
