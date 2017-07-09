defmodule RedisScriptsTest do
  use ExUnit.Case

  @lpop_rpush_src_dest_script File.read!("#{:code.priv_dir(:verk)}/lpop_rpush_src_dest.lua")
  @mrpop_lpush_src_dest_script File.read!("#{:code.priv_dir(:verk)}/mrpop_lpush_src_dest.lua")
  @enqueue_retriable_job_script File.read!("#{:code.priv_dir(:verk)}/enqueue_retriable_job.lua")
  @requeue_job_now_script File.read!("#{:code.priv_dir(:verk)}/requeue_job_now.lua")

  setup do
    { :ok, redis } = Confex.get_env(:verk, :redis_url) |> Redix.start_link
    { :ok, %{ redis: redis } }
  end

  describe "lpop_rpush_src_dest" do
    test "lpop from source rpush to dest total", %{ redis: redis } do
      { :ok, _ } = Redix.command(redis, ~w(DEL source dest))
      { :ok, _ } = Redix.command(redis, ~w(RPUSH dest job1 job2 job3))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH source job4 job5 job6))
      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, ["job6", "job5", "job4"] }

      assert Redix.command(redis, ["EVAL", @lpop_rpush_src_dest_script, 2, "source", "dest", 10]) == {:ok, [0, 3] }

      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, [] }
      assert Redix.command(redis, ~w(LRANGE dest 0 -1)) == { :ok, ~w(job1 job2 job3 job6 job5 job4) }
    end

    test "lpop from source rpush to dest partial", %{ redis: redis } do
      { :ok, _ } = Redix.command(redis, ~w(DEL source dest))
      { :ok, _ } = Redix.command(redis, ~w(RPUSH dest job1 job2 job3))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH source job4 job5 job6))
      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, ["job6", "job5", "job4"] }

      assert Redix.command(redis, ["EVAL", @lpop_rpush_src_dest_script, 2, "source", "dest", 1]) == {:ok, [2, 1] }

      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, ["job5", "job4"] }
      assert Redix.command(redis, ~w(LRANGE dest 0 -1)) == { :ok, ~w(job1 job2 job3 job6) }
    end
  end

  describe "mrpop_lpush_src_dest" do
    test "mrpop from source lpush to dest", %{ redis: redis } do
      { :ok, _ } = Redix.command(redis, ~w(DEL source dest))
      { :ok, _ } = Redix.command(redis, ~w(RPUSH dest job1 job2 job3))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH source job4 job5 job6))

      assert Redix.command(redis, ["EVAL", @mrpop_lpush_src_dest_script, 2, "source", "dest", 2]) == {:ok, ["job4", "job5"]}
      assert Redix.command(redis, ["EVAL", @mrpop_lpush_src_dest_script, 2, "source", "dest", 2]) == {:ok, ["job6"]}
      assert Redix.command(redis, ["EVAL", @mrpop_lpush_src_dest_script, 2, "source", "dest", 2]) == {:ok, []}

      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, [] }
      assert Redix.command(redis, ~w(LRANGE dest 0 -1)) == { :ok, ~w(job6 job5 job4 job1 job2 job3) }
    end
  end

  describe "enqueue_retriable_job" do
    test "enqueue job to queue form retry set", %{ redis: redis } do
      job = "{\"jid\":\"123\",\"enqueued_at\":\"42\",\"queue\":\"test_queue\"}"
      other_job = "{\"jid\":\"456\",\"queue\":\"test_queue\"}"
      enqueued_job = "{\"jid\":\"789\",\"queue\":\"test_queue\"}"

      { :ok, _ } = Redix.command(redis, ~w(DEL retry queue:test_queue))
      { :ok, _ } = Redix.command(redis, ~w(ZADD retry 42 #{job} 45 #{other_job}))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH queue:test_queue #{enqueued_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "41"]) == { :ok, nil }
      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "42"]) == { :ok, job }

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [other_job, "45"] }
      assert Redix.command(redis, ~w(LRANGE queue:test_queue 0 -1)) == { :ok, [job, enqueued_job] }
    end

    test "enqueue job to queue form schedule set", %{ redis: redis } do
      schedulled_job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      enqueued_shedulled_job = "{\"jid\":\"123\",\"enqueued_at\":\"42\",\"queue\":\"test_queue\"}"

      { :ok, _ } = Redix.command(redis, ~w(DEL schedule queue:test_queue))
      { :ok, _ } = Redix.command(redis, ~w(ZADD schedule 42 #{schedulled_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "41"]) == { :ok, nil }
      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "schedule", "42"]) == { :ok, enqueued_shedulled_job }
    end
  end

  describe "requeue_job_now" do
    test "improper job format returns job data doesn't move job", %{ redis: redis } do
      job_with_no_queue = "{\"jid\":\"123\"}"
      { :ok, _ } = Redix.command(redis, ~w(DEL retry queue:test_queue))
      { :ok, _ } = Redix.command(redis, ~w(ZADD retry 42 #{job_with_no_queue}))

      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job_with_no_queue]) == {:ok, job_with_no_queue}
      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [job_with_no_queue, "42"] }
    end

    test "valid job format, requeue job moves from retry to original queue", %{ redis: redis } do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      { :ok, _ } = Redix.command(redis, ~w(DEL retry queue:test_queue))
      { :ok, _ } = Redix.command(redis, ~w(ZADD retry 42 #{job}))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [job, "42"] }
      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) == { :ok, job }
      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [] }
    end

    test "valid job format, empty original queue job is still requeued", %{ redis: redis } do
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      { :ok, _ } = Redix.command(redis, ~w(DEL retry queue:test_queue))

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [] }
      assert Redix.command(redis, ["EVAL", @requeue_job_now_script, 1, "retry", job]) == { :ok, job }
      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [] }
    end
  end
end
