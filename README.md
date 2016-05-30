[![Build Status](https://travis-ci.org/edgurgel/verk.svg?branch=master)](https://travis-ci.org/edgurgel/verk)
[![Hex pm](http://img.shields.io/hexpm/v/verk.svg?style=flat)](https://hex.pm/packages/verk)
[![Coverage Status](https://coveralls.io/repos/edgurgel/verk/badge.svg?branch=master&service=github)](https://coveralls.io/github/edgurgel/verk?branch=master)
Verk
===

Verk is a job processing system backed by Redis. It uses the same job definition of Sidekiq/Resque.

The goal is to be able to isolate the execution of a queue of jobs as much as possible.

Every queue has its own supervision tree:

* A pool of workers;
* A `QueueManager` that interacts with Redis to get jobs and enqueue them back to be retried if necessary;
* A `WorkersManager` that will interact with the `QueueManager` and the pool to execute jobs.

Verk will hold one connection to Redis per queue plus one dedicated to the `ScheduleManager` and one general connection for other use cases like deleting a job from retry set or enqueuing new jobs.

The `ScheduleManager` fetches jobs from the `retry` set to be enqueued back to the original queue when it's ready to be retried.

It also has one GenEvent manager called `EventManager`.

The image below is an overview of Verk's supervision tree running with a queue named `default` having 5 workers.

![Supervision Tree](http://i.imgur.com/8BW8D04.png)

Feature set:

* Retry mechanism
* Dynamic addition/removal of queues
* Reliable job processing (RPOPLPUSH and Lua scripts to the rescue)
* Error and event tracking

## Installation

First, add Verk to your `mix.exs` dependencies:

```elixir
def deps do
  [{:verk, "~> 0.12"}]
end
```

and run `$ mix deps.get`. Now, list the `:verk` application as your
application dependency:

```elixir
def application do
  [applications: [:verk]]
end
```

Finally add `Verk.Supervisor` to your supervision tree:

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

Verk was tested using Redis 2.8+

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
Verk.enqueue(%Verk.Job{queue: :default, class: "ExampleWorker", args: [1,2]})
```

This job can also be scheduled using `Verk.schedule/2`:

 ```elixir
 perform_at = Timex.shift(Timex.DateTime.now, seconds: 30)
 Verk.schedule(%Verk.Job{queue: :default, class: "ExampleWorker", args: [1,2]}, perform_at)
 ```

## Configuration

Example configuration for verk having 2 queues: `default` and `priority`

The queue `default` will have a maximum of 25 jobs being processed at a time and `priority` just 10.

```elixir
config :verk, queues: [default: 25, priority: 10],
              poll_interval: 5000,
              node_id: "1",
              redis_url: "redis://127.0.0.1:6379"
```

The configuration for releases is still a work in progress.

## Queues

It's possible to dynamically add and remove queues from Verk.

```elixir
Verk.add_queue(:new, 10) # Adds a queue named `new` with 10 workers
```

```elixir
Verk.remove_queue(:new) # Terminate and delete the queue named `new`
```

## Reliability

Verk's goal is to never have a job that exists only in memory. It uses Redis as the single source of truth to retry and track jobs that were being processed if some crash happened.

Verk will re-enqueue jobs if the application crashed while jobs were running. It will also retry jobs that failed keeping track of the errors that happened.

The jobs that will run on top of Verk should be idempotent as they may run more than once.

## Error tracking

One can track when jobs start and finish or fail. This can be useful to build metrics around the jobs. The `QueueStats` handler does some kind of metrics using these events: https://github.com/edgurgel/verk/blob/master/lib/verk/queue_stats.ex

Verk has an Event Manager that notify the following events:

* `Verk.Events.JobStarted`
* `Verk.Events.JobFinished`
* `Verk.Events.JobFailed`

Here is an example of a `GenEvent` handler to print any event:

```elixir
defmodule PrintHandler do
  use GenEvent

  def handle_event(event, state) do
    IO.puts "Event received: #{inspect event}"
    { :ok, state }
  end
end
```

One can define an error tracking handler like this:

```elixir
defmodule TrackingErrorHandler do
  use GenEvent

  def handle_event(%Verk.Events.JobFailed{job: job, failed_at: failed_at, stacktrace: trace}, state) do
    MyTrackingExceptionSystem.track(stacktrace: trace, name: job.class)
    { :ok, state }
  end
  def handle_event(_, state) do
    # Ignore other events
    { :ok, state }
  end
end
```

You also need to add the handler to connect with the event manager:

```elixir
GenEvent.add_mon_handler(Verk.EventManager, TrackingErrorHandler, [])
```

More info about `GenEvent.add_mon_handler/3` [here](http://elixir-lang.org/docs/v1.1/elixir/GenEvent.html#add_mon_handler/3).

## Dashboard ?

Check [Verk Web](https://github.com/edgurgel/verk_web)!

![](http://i.imgur.com/AclG57m.png)

## Sponsorship

Initial development sponsored by [Carnival.io](http://carnival.io)
