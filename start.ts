import pMap from "p-map";
import { log } from "@util/log";
import { config } from "@util/config";
import { createStreamHelper } from "@util/redis";
import { createOrder } from "@util/orders";
import { redis } from "./redis";

const streams = createStreamHelper(redis);

const ensureGroups = async () => {
  for (const group of config.groups) {
    await streams.createGroup({
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
    const eventId = await streams.publishEvent({
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
  const allConsumers = config.groups.reduce((res, group) => {
    res.push(
      ...group.consumers.map((consumer) => ({
        group: group.name,
        consumer,
      }))
    );
    return res;
  }, []);

  pMap(allConsumers, async ({ group, consumer }) => {
    await streams.startConsumer(
      {
        stream: config.stream,
        group: group,
        consumer,
      },
      async ({ id, data }) => {
        log.info(
          {
            group: group,
            consumer,
            stream: config.stream,
            eventId: id,
            data,
          },
          "Consumer group is processing message."
        );
      }
    );
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
