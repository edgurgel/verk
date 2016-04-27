use Mix.Config

config :verk, queues: [{:default, 25}], poll_interval: 5000, node_id: "1", redis_url: "redis://127.0.0.1:6379", workers_manager_timeout: 1200
config :logger, :console,
  format: "\n$date $time [$level] $metadata$message\n",
  metadata: [:process_id]

if Mix.env == :test do
  config :logger, backends: [] # Silent logging for tests
  config :tzdata, :autoupdate, :disabled
  config :verk, queues: [{:default, 1}], redis_url: "redis://127.0.0.1:6379/1"
end

