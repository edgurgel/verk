![Verk](https://i.imgur.com/unSd0Zr.png)
#  [![Build Status](https://travis-ci.org/edgurgel/verk.svg?branch=master)](https://travis-ci.org/edgurgel/verk) [![Hex pm](http://img.shields.io/hexpm/v/verk.svg?style=flat)](https://hex.pm/packages/verk) [![Coverage Status](https://coveralls.io/repos/edgurgel/verk/badge.svg?branch=master&service=github)](https://coveralls.io/github/edgurgel/verk?branch=master) [![hex.pm downloads](https://img.shields.io/hexpm/dt/verk.svg?style=flat)](https://hex.pm/packages/verk)

Verk is a job processing system backed by Redis. It uses the same job definition of Sidekiq/Resque.

The goal is to be able to isolate the execution of a queue of jobs as much as possible.

Every queue has its own supervision tree:

* A pool of workers;
* A `QueueManager` that interacts with Redis to get jobs and enqueue them back to be retried if necessary;
* A `WorkersManager` that will interact with the `QueueManager` and the pool to execute jobs.

Verk will hold one connection to Redis per queue plus one dedicated to the `ScheduleManager` and one general connection for other use cases like deleting a job from retry set or enqueuing new jobs.

The `ScheduleManager` fetches jobs from the `retry` set to be enqueued back to the original queue when it's ready to be retried.

It also has one GenStage producer called `Verk.EventProducer`.

The image below is an overview of Verk's supervision tree running with a queue named `default` having 5 workers.

![Supervision Tree](https://i.imgur.com/1vzAVfZ.png)

Feature set:

* Retry mechanism with exponential backoff
* Dynamic addition/removal of queues
* Reliable job processing (RPOPLPUSH and Lua scripts to the rescue)
* Error and event tracking

## Installation

First, add Verk to your `mix.exs` dependencies:

```elixir
def deps do
  [{:verk, "~> 1.0"}]
end
```

and run `$ mix deps.get`. Add `:verk` to your applications list if your Elixir version is 1.3 or lower:

```elixir
def application do
  [applications: [:verk]]
end
```

Add `Verk.Supervisor` to your supervision tree:

```elixir
defmodule Example.App do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec
    tree = [supervisor(Verk.Supervisor, [])]
    opts = [name: Simple.Sup, strategy: :one_for_one]
    Supervisor.start_link(tree, opts)
  end
end
```

Finally we need to configure how Verk will process jobs.

## Configuration

Example configuration for Verk having 2 queues: `default` and `priority`

The queue `default` will have a maximum of 25 jobs being processed at a time and `priority` just 10.

```elixir
config :verk, queues: [default: 25, priority: 10],
              max_retry_count: 10,
              max_dead_jobs: 100,
              poll_interval: 5000,
              start_job_log_level: :info,
              done_job_log_level: :info,
              fail_job_log_level: :info,
              node_id: "1",
              redis_url: "redis://127.0.0.1:6379"
```

Verk supports the convention `{:system, "ENV_NAME", default}` for reading environment configuration at runtime using [Confex](https://hexdocs.pm/confex/readme.html):

```elixir
config :verk, queues: [default: 25, priority: 10],
              max_retry_count: 10,
              max_dead_jobs: 100,
              poll_interval: {:system, :integer, "VERK_POLL_INTERVAL", 5000},
              start_job_log_level: :info,
              done_job_log_level: :info,
              fail_job_log_level: :info,
              node_id: "1",
              redis_url: {:system, "VERK_REDIS_URL", "redis://127.0.0.1:6379"}
```

Now Verk is ready to start processing jobs! :tada:

## Workers

A job is defined by a module and arguments:

```elixir
defmodule ExampleWorker do
  def perform(arg1, arg2) do
    arg1 + arg2
  end
end
```

This job can be enqueued using `Verk.enqueue/1`:

```elixir
Verk.enqueue(%Verk.Job{queue: :default, class: "ExampleWorker", args: [1,2], max_retry_count: 5})
```

This job can also be scheduled using `Verk.schedule/2`:

 ```elixir
 perform_at = Timex.shift(Timex.now, seconds: 30)
 Verk.schedule(%Verk.Job{queue: :default, class: "ExampleWorker", args: [1,2]}, perform_at)
 ```

### Retry at

A job can define the function `retry_at/2` for custom retry time delay:

```elixir
defmodule ExampleWorker do
  def perform(arg1, arg2) do
    arg1 + arg2
  end

  def retry_at(failed_at, retry_count) do
    failed_at + retry_count
  end
end
```

In this example, the first retry will be scheduled a second later,
the second retry will be scheduled two seconds later, and so on.

If `retry_at/2` is not defined the default exponential backoff is used.

### Keys in arguments

By default, Verk will decode keys in arguments to binary strings.
You can change this behavior for jobs enqueued by Verk with the following configuration:
```elixir
config :verk, :args_keys, value
```

The following values are valid:

* `:strings` (default) - decodes keys as binary strings
* `:atoms` - keys are converted to atoms using `String.to_atom/1`
* `:atoms!` - keys are converted to atoms using `String.to_existing_atom/1`


## Queues

It's possible to dynamically add and remove queues from Verk.

```elixir
Verk.add_queue(:new, 10) # Adds a queue named `new` with 10 workers
```

```elixir
Verk.remove_queue(:new) # Terminate and delete the queue named `new`
```

## Deployment

The way Verk currently works, there are two pitfalls to pay attention to:

1. **Each worker node's `node_id` MUST be unique.** If a node goes online with a
  `node_id`, which is already in use by another running node, then the second
  node will re-enqueue all jobs currently in progress on the first node, which
  results in jobs executed multiple times.
2. **Take caution around removing nodes.** If a node with jobs in progress is
  killed, those jobs will not be restarted until another node with the same
  `node_id` comes online. If another node with the same `node_id` never comes
  online, the jobs will be stuck forever. This means you should not use dynamic
  `node_id`s such as Docker container ids or Kubernetes Deployment pod names.

### On Heroku

Heroku provides
[an experimental environment variable](https://devcenter.heroku.com/articles/dynos#local-environment-variables)
named after the type and number of the dyno.

```elixir
config :verk,
  node_id: {:system, "DYNO", "job.1"}
```

_It is possible that two dynos with the same name could overlap for a short time
during a dyno restart._ As the Heroku documentation says:

> [...] $DYNO is not guaranteed to be unique within an app. For example, during
> a deploy or restart, the same dyno identifier could be used for two running
> dynos. It will be eventually consistent, however.

This means that you are still at risk of violating the first rule above on
`node_id` uniqueness. A slightly naive way of lowering the risk would be to
add a delay in your application before the Verk queue starts.

### On Kubernetes

We recommend using a
[StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
to run your pool of workers. StatefulSets add a label,
`statefulset.kubernetes.io/pod-name`, to all its pods with the value
`{name}-{n}`, where `{name}` is the name of your StatefulSet and `{n}` is a
number from 0 to `spec.replicas - 1`. StatefulSets maintain a sticky identity
for its pods and guarantee that two identical pods are never up simultaneously.
This way it satisfies both of our deployment rules mentioned above.

Define your worker like this:

```yaml
# StatefulSets require a service, even though we don't use it directly for anything
apiVersion: v1
kind: Service
metadata:
 name: my-worker
 labels:
   app: my-worker
spec:
 clusterIP: None
 selector:
   app: my-worker

---

apiVersion: apps/v1
kind: StatefulSet
metadata:
 name: my-worker
 labels:
   app: my-worker
spec:
 selector:
   matchLabels:
     app: my-worker
 serviceName: my-worker
 # We run two workers in this example
 replicas: 2
 # The workers don't depend on each other, so we can use Parallel pod management
 podManagementPolicy: Parallel
 template:
   metadata:
     labels:
       app: my-worker
   spec:
     # This should probably match up with the setting you used for Verk's :shutdown_timeout
     terminationGracePeriodSeconds: 30
     containers:
       - name: my-worker
         image: my-repo/my-worker
         env:
           - name: VERK_NODE_ID
             valueFrom:
               fieldRef:
                 fieldPath: metadata.labels['statefulset.kubernetes.io/pod-name']
```

Notice how we use a `fieldRef` to expose the pod's
`statefulset.kubernetes.io/pod-name` label as the `VERK_NODE_ID` environment
variable. Instruct Verk to use this environment variable as `node_id`:

```elixir
config :verk,
 node_id: {:system, "VERK_NODE_ID"}
```

Be careful when scaling the number of `replicas` down. Make sure that the pods
that will be stopped and never come back do not have any jobs in progress.
Scaling up is always safe.

Don't use Deployments for pods that will run Verk. If you hardcode `node_id`
into your config, multiple pods with the same `node_id`will be online at the
same time, violating the first rule. If you use a non-sticky environment
variable, such as `HOSTNAME`, you'll violate the second rule and cause jobs to
get stuck every time you deploy.

If your application serves as e.g. both an API and Verk queue, then it may be
wise to run a separate Deployment for your API, which does not run Verk. In that
case you can configure your application to check an environment variable,
`VERK_DISABLED`, for whether it should handle any Verk queues:

```elixir
# In your config.exs
config :verk,
  queues: {:system, {MyApp.Env, :verk_queues, []}, "VERK_DISABLED"}

# In some other file
defmodule MyApp.Env do
  def verk_queues("true"), do: {:ok, []}
  def verk_queues(_), do: {:ok, [default: 25, priority: 10]}
end
```

Then set `VERK_DISABLED=true` in your Deployment's spec.

### EXPERIMENTAL - Generate Node ID

Since Verk 1.6.0 there is a new experimental optional configuration `generate_node_id`. Node IDs are completely controlled automatically by Verk if this configuration is set to `true`. 


#### Under the hood

* Each time a job is moved to the list of jobs inprogress of a queue this node is added to `verk_nodes` (`SADD verk_nodes node_id`) and the queue is added to `verk:node:#{node_id}:queues` (`SADD verk:node:123:queues queue_name`)

* Each frequency milliseconds we set the node key to expire in 2 * frequency
`PSETEX verk:node:#{node_id} 2 * frequency alive`

* Each frequency milliseconds check for all the keys of all nodes (`verk_nodes`). If the key expired it means that this node is dead and it needs to have its jobs restored.

To restore we go through all the running queues (`verk:node:#{node_id}:queues`) of that node and enqueue them from inprogress back to the queue. Each "enqueue back from in progress" is atomic (<3 lua) so we won't have duplicates.

#### Configuration

The default `frequency` is 30_000 milliseconds but it can be changed by setting the configuration key `heartbeat`.

```elixir
config :verk,
  queues: [default: 5, priority: 5],
  redis_url: "redis://127.0.0.1:6379",
  generate_node_id: true,
  heartbeat: 30_000,
```

## Reliability

Verk's goal is to never have a job that exists only in memory. It uses Redis as the single source of truth to retry and track jobs that were being processed if some crash happened.

Verk will re-enqueue jobs if the application crashed while jobs were running. It will also retry jobs that failed keeping track of the errors that happened.

The jobs that will run on top of Verk should be idempotent as they may run more than once.

## Error tracking

One can track when jobs start and finish or fail. This can be useful to build metrics around the jobs. The `QueueStats` handler does some kind of metrics using these events: https://github.com/edgurgel/verk/blob/master/lib/verk/queue_stats.ex

Verk has an Event Manager that notifies the following events:

* `Verk.Events.JobStarted`
* `Verk.Events.JobFinished`
* `Verk.Events.JobFailed`
* `Verk.Events.QueueRunning`
* `Verk.Events.QueuePausing`
* `Verk.Events.QueuePaused`

One can define an error tracking handler like this:

```elixir
defmodule TrackingErrorHandler do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  def init(_) do
    filter = fn event -> event.__struct__ == Verk.Events.JobFailed end
    {:consumer, :state, subscribe_to: [{Verk.EventProducer, selector: filter}]}
  end

  def handle_events(events, _from, state) do
    Enum.each(events, &handle_event/1)
    {:noreply, [], state}
  end

  defp handle_event(%Verk.Events.JobFailed{job: job, failed_at: failed_at, stacktrace: trace}) do
    MyTrackingExceptionSystem.track(stacktrace: trace, name: job.class)
  end
end
```

Notice the selector to get just the type JobFailed. If no selector is set every event is sent.

Then adding the consumer to your supervision tree:

  ```elixir
  defmodule Example.App do
    use Application

    def start(_type, _args) do
      import Supervisor.Spec
      tree = [supervisor(Verk.Supervisor, []),
              worker(TrackingErrorHandler, [])]
      opts = [name: Simple.Sup, strategy: :one_for_one]
      Supervisor.start_link(tree, opts)
    end
  end
  ```

## Dashboard ?

Check [Verk Web](https://github.com/edgurgel/verk_web)!

![Dashboard](http://i.imgur.com/LsDKIVT.png)

## Metrics ?

Check [Verk Stats](https://github.com/edgurgel/verk-stats)

## Sponsorship

Initial development sponsored by [Carnival.io](http://carnival.io)
