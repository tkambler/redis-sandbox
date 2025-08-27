const config = {
  stream: "orders",
  groups: [
    {
      name: "group1",
      consumers: ["fooConsumer", "barConsumer"],
    },
    {
      name: "group2",
      consumers: ["herpConsumer", "derpConsumer"],
    },
  ],
};

export { config };
