defmodule RedisScriptsTest do
  use ExUnit.Case, async: true

  @enqueue_retriable_job_script File.read!("#{:code.priv_dir(:verk)}/enqueue_retriable_job.lua")
  @requeue_job_now_script File.read!("#{:code.priv_dir(:verk)}/requeue_job_now.lua")
  @reenqueue_pending_job_script File.read!("#{:code.priv_dir(:verk)}/reenqueue_pending_job.lua")

  setup do
    {:ok, redis} = Confex.get_env(:verk, :redis_url) |> Redix.start_link()
    {:ok, %{redis: redis}}
  end

  describe "enqueue_retriable_job" do
    test "enqueue job to queue form retry set", %{redis: redis} do
      job = "{\"jid\":\"123\",\"enqueued_at\":\"42\",\"queue\":\"test_queue\"}"
      other_job = "{\"jid\":\"456\",\"queue\":\"test_queue\"}"
      enqueued_job = "{\"jid\":\"789\",\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL retry verk:queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job} 45 #{other_job}))
      {:ok, _} = Redix.command(redis, ~w(XADD verk:queue:test_queue * job #{enqueued_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "42"]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [other_job, "45"]}

      assert [[_, ["job", ^enqueued_job]], [_, ["job", ^job]]] =
               Redix.command!(redis, ~w(XRANGE verk:queue:test_queue - +))
    end

    test "enqueue job to queue form schedule set", %{redis: redis} do
      scheduled_job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      enqueued_scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":42,\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL schedule verk:queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD schedule 42 #{scheduled_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "42"]) ==
               {:ok, enqueued_scheduled_job}

      assert [[_, ["job", ^enqueued_scheduled_job]]] =
               Redix.command!(redis, ~w(XRANGE verk:queue:test_queue - +))
    end

    test "enqueue job to queue form null enqueued_at key", %{redis: redis} do
      scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":null,\"queue\":\"test_queue\"}"
      enqueued_scheduled_job = "{\"jid\":\"123\",\"enqueued_at\":42,\"queue\":\"test_queue\"}"

      {:ok, _} = Redix.command(redis, ~w(DEL schedule verk:queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD schedule 42 #{scheduled_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "41"]) ==
               {:ok, nil}

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "42"]) ==
               {:ok, enqueued_scheduled_job}

      assert [[_, ["job", ^enqueued_scheduled_job]]] =
               Redix.command!(redis, ~w(XRANGE verk:queue:test_queue - +))
    end
  end

  describe "reenqueue_pending_job_script" do
    test "claim and reenqueue pending job", %{redis: redis} do
      job = "{\"jid\":\"123\"}"
      stream = "verk:queue:test_queue"
      Redix.command!(redis, ["FLUSHDB"])
      Redix.command!(redis, ~w(XADD #{stream} * job #{job}))
      Redix.command(redis, ["XGROUP", "CREATE", stream, "verk", 0, "MKSTREAM"])
      Redix.command!(redis, ~w(XREADGROUP GROUP verk node_123 COUNT 1 STREAMS #{stream} >))
      [[id, _, idle_time, _]] = Redix.command!(redis, ~w(XPENDING #{stream} verk - + 1))

      {:ok, 1} =
        Redix.command(redis, [
          "EVAL",
          @reenqueue_pending_job_script,
          1,
          stream,
          "verk",
          id,
          idle_time
        ])

      assert [[^stream, [[new_id, ["job", ^job]]]]] =
               Redix.command!(redis, ~w(XREAD COUNT 100 STREAMS #{stream} 0-0))

      assert new_id != id
    end

    test "claim and reenqueue pending job when already claimed", %{redis: redis} do
      job = "{\"jid\":\"123\"}"
      stream = "verk:queue:test_queue"
      Redix.command!(redis, ["FLUSHDB"])
      Redix.command!(redis, ~w(XADD #{stream} * job #{job}))
      Redix.command(redis, ["XGROUP", "CREATE", stream, "verk", 0, "MKSTREAM"])
      Redix.command!(redis, ~w(XREADGROUP GROUP verk node_123 COUNT 1 STREAMS #{stream} >))
      [[id, _, idle_time, _]] = Redix.command!(redis, ~w(XPENDING #{stream} verk - + 1))

      {:ok, 1} =
        Redix.command(redis, [
          "EVAL",
          @reenqueue_pending_job_script,
          1,
          stream,
          "verk",
          id,
          idle_time
        ])

      assert [[^stream, [[new_id, ["job", ^job]]]]] =
               Redix.command!(redis, ~w(XREAD COUNT 100 STREAMS #{stream} 0-0))

      {:ok, 0} =
        Redix.command(redis, [
          "EVAL",
          @reenqueue_pending_job_script,
          1,
          stream,
          "verk",
          id,
          idle_time
        ])

      assert new_id != id
    end
  end

  describe "requeue_job_now" do
    test "improper job format returns job data doesn't move job", %{redis: redis} do
      job_with_no_queue = "{\"jid\":\"123\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry verk:queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job_with_no_queue}))

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job_with_no_queue]) ==
               {:ok, job_with_no_queue}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [job_with_no_queue, "42"]}
    end

    test "valid job format, requeue job moves from retry to original queue", %{redis: redis} do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry verk:queue:test_queue))
      {:ok, _} = Redix.command(redis, ~w(ZADD retry 42 #{job}))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) ==
               {:ok, [job, "42"]}

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}

      assert [[_, ["job", ^job]]] = Redix.command!(redis, ~w(XRANGE verk:queue:test_queue - +))
    end

    test "valid job format, empty original queue job is still requeued", %{redis: redis} do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      {:ok, _} = Redix.command(redis, ~w(DEL retry verk:queue:test_queue))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) ==
               {:ok, job}

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == {:ok, []}

      assert [[_, ["job", ^job]]] = Redix.command!(redis, ~w(XRANGE verk:queue:test_queue - +))
    end
  end
end
