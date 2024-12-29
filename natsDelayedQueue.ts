import {
  StringCodec,
  JetStreamClient,
  NatsConnection,
  JetStreamManager,
  StreamConfig,
  headers,
  RetentionPolicy,
  StorageType,
  AckPolicy,
  nanos,
} from "nats";

type Options = {
  streamName: string;
  subject: string;
  consumerName?: string;

  /**
   * @default 2 min.
   */
  duplicateWindowInSec?: number;

  onMessageDeliveryIssue?: (e: {
    msgContent: string;
    deliveryTime: number;
    retryCount: number;
    error: Error;
    timestamp: number;
  }) => void;

  messageDeliveryIssueTriggerMod?: number;
};

export class NatsDelayedQueue<TMessage = any> {
  private js: JetStreamClient;
  private jsm: JetStreamManager;
  private sc = StringCodec();
  private isSetupCompleted = false;

  constructor(private nc: NatsConnection, private options: Options) {
    this.options.consumerName ??= this.options.streamName + "Consumer";
  }

  async setup(): Promise<void> {
    if (this.isSetupCompleted) {
      return;
    }

    const { streamName, subject, duplicateWindowInSec = 0 } = this.options;

    this.js = this.nc.jetstream();
    this.jsm = await this.nc.jetstreamManager({});

    try {
      await this.jsm.streams.info(streamName);
    } catch {
      const config: Partial<StreamConfig> = {
        name: streamName,
        subjects: [subject],
        retention: RetentionPolicy.Workqueue,
        storage: StorageType.File,
        duplicate_window: duplicateWindowInSec
          ? nanos(1000 * duplicateWindowInSec)
          : undefined,
      };

      await this.jsm.streams.add(config);
    }

    this.isSetupCompleted = true;
  }

  async sendDelayedMessage<T = TMessage>(
    message: T,
    delayMilliseconds: number = 0,
    uniqueId?: string
  ): Promise<boolean> {
    const { subject } = this.options;

    const deliveryTime = Date.now() + delayMilliseconds;
    const hdrs = headers();
    hdrs.append("Delivery-Time", deliveryTime.toString());

    const stringMessage = JSON.stringify(message);

    const res = await this.js.publish(subject, this.sc.encode(stringMessage), {
      headers: hdrs,
      msgID: uniqueId,
    });

    return !res.duplicate;
  }

  async receiveMessages<T = TMessage>(
    handler: (message: T, retryCount: number) => Promise<void>
  ): Promise<void> {
    const {
      streamName,
      consumerName,
      onMessageDeliveryIssue,
      messageDeliveryIssueTriggerMod = 5,
    } = this.options;

    if (!this.isSetupCompleted) {
      await this.setup();
      this.isSetupCompleted = true;
    }

    await this.ensureConsumerExists();

    const consumer = await this.nc
      .jetstream()
      .consumers.get(streamName, consumerName);

    const messages = await consumer.consume({});

    for await (const msg of messages) {
      const currentTime = Date.now();
      const deliveryTime = parseInt(
        msg.headers?.get("Delivery-Time") || "0",
        10
      );

      // check if delivery time was provided
      if (!deliveryTime) {
        const messageContent = this.sc.decode(msg.data);

        onMessageDeliveryIssue?.({
          msgContent: messageContent,
          deliveryTime,
          retryCount: 0,
          error: new Error("Delivery-Time header not found"),
          timestamp: Date.now(),
        });

        msg.ack();
        continue;
      }

      // check if it's not time yet, `nak` with delay
      if (currentTime < deliveryTime) {
        const delay = deliveryTime - currentTime;

        console.log("requeued", delay);

        // Requeue with remaining delay
        msg.nak(delay);
        continue;
      }

      // process message
      const retryCount = msg.info.redeliveryCount - 1;

      let messageContent: string = "";

      try {
        messageContent = this.sc.decode(msg.data);

        const content = JSON.parse(messageContent);

        await handler(content, retryCount);

        msg.ack();
      } catch (err) {
        if (retryCount % messageDeliveryIssueTriggerMod === 0) {
          onMessageDeliveryIssue?.({
            msgContent: messageContent,
            deliveryTime,
            retryCount,
            error: err,
            timestamp: Date.now(),
          });
        }

        msg.nak((retryCount + 1) * 500);
      }
    }
  }

  private async ensureConsumerExists() {
    const { streamName, consumerName } = this.options;

    try {
      await this.jsm.consumers.info(streamName, consumerName!);
    } catch {
      await this.jsm.consumers.add(streamName, {
        ack_policy: AckPolicy.Explicit,
        durable_name: consumerName,
        ack_wait: nanos(5000),
        backoff: [
          nanos(100),
          nanos(1000),
          nanos(2000),
          nanos(3000),
          nanos(5000),
        ],
      });
    }
  }
}
