import { connect, jwtAuthenticator, StringCodec } from "nats";
import { NatsDelayedQueue } from "./natsDelayedQueue";

const { USER_JWT, USER_SEED } = process.env;

// Example usage
async function main() {
  const sc = StringCodec();
  const nc = await connect({
    servers: ["nats://localhost:4222"],
    authenticator: jwtAuthenticator(USER_JWT!, sc.encode(USER_SEED!)),
  });
  console.log("Connected to NATS");

  const disconnect = async () => {
    await nc?.drain();
    await nc?.close();
  };

  const queue = new NatsDelayedQueue(nc, {
    streamName: "TIMER_MESSAGES2",
    subject: "timer.callbacks2",
    consumerName: "timerConsumer",

    duplicateWindowInSec: 0,

    messageDeliveryIssueTriggerMod: 1,
    onMessageDeliveryIssue({ error, ...e }) {
      console.warn("Failed", e, error.message);
    },
  });

  await queue.setup();

  try {
    // Set up message handler
    queue.receiveMessages(async (message, retryCount) => {
      // if (retryCount < 2) {
      //   throw new Error("Oops");
      // }

      console.log(
        "Done ✅",
        message.hello,
        retryCount,
        (Date.now() - startedAt) / 1000
      );
    });

    // Send test messages with different delays
    console.log("Sending messages...");
    const startedAt = Date.now();

    await queue.sendDelayedMessage({ hello: "world" }, 0, "m5");
    await queue.sendDelayedMessage({ hello: "world2" }, 0, "m5");

    // Keep running for demo
    await new Promise((resolve) => setTimeout(resolve, 20000));
    await disconnect();
  } catch (error) {
    console.error("Error:", error);
    await disconnect();
  }
}

main().catch(console.error);
