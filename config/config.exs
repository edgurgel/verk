use Mix.Config

config :verk,
  queues: [{:default, 25}],
  poll_interval: 5000,
  node_id: "1",
  redis_url: "redis://127.0.0.1:6379",
  shutdown_timeout: 60_000,
  consumer_timeout: 30_000

# failed_job_stacktrace_size: 5

config :logger, :console,
  format: "\n$date $time [$level] $metadata$message\n",
  metadata: [:process_id]

if Mix.env() == :test do
  # Silent logging for tests
  config :logger, backends: []
  config :verk, queues: [{:default, 1}], redis_url: "redis://127.0.0.1:6379/1"
end
