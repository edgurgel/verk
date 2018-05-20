use Mix.Config

config :verk, queues: [queue_one: 5, queue_two: 5],
              redis_url: "redis://127.0.0.1:6379/5",
              shutdown_timeout: 3_000
