import pMap from "p-map";
import { log } from "@util/log";
import { config } from "@util/config";
import { publishEvent, createGroup, startConsumer } from "@util/redis";
import { createOrder } from "@util/orders";

const ensureGroups = async () => {
  for (const group of config.groups) {
    await createGroup({
      group: group.name,
      stream: config.stream,
    });
  }
};

const createProducer = async () => {
  const createEvent = async () => {
    const order = createOrder();
    const data = {
      order,
    };
    const eventId = await publishEvent({
      stream: config.stream,
      data,
    });
    log.info(
      { data, stream: config.stream, event_id: eventId },
      "New order event published to stream."
    );
  };
  setInterval(async () => {
    await createEvent();
  }, 4000);
};

const createConsumers = async () => {
  pMap(config.groups, async (group) => {
    await pMap(group.consumers, async (consumer) => {
      await startConsumer(
        {
          stream: config.stream,
          group: group.name,
          consumer,
        },
        async ({ id, data }) => {
          log.info(
            {
              group: group.name,
              consumer,
              stream: config.stream,
              eventId: id,
              data,
            },
            "Consumer group is processing message."
          );
        }
      );
    });
  }).catch((err) => {
    throw new Error("Failed to start consumers", {
      cause: err,
    });
  });
};

(async () => {
  log.info("Ensuring groups exist...");
  await ensureGroups();

  log.info("Creating producer...");
  await createProducer();

  log.info("Creating consumers...");
  await createConsumers();
})();
