defmodule RedisScriptsTest do
  use ExUnit.Case

  @lpop_rpush_src_dest_script File.read!("#{:code.priv_dir(:verk)}/lpop_rpush_src_dest.lua")
  @mrpop_lpush_src_dest_script File.read!("#{:code.priv_dir(:verk)}/mrpop_lpush_src_dest.lua")
  @enqueue_retriable_job_script File.read!("#{:code.priv_dir(:verk)}/enqueue_retriable_job.lua")

  setup do
    { :ok, redis } = Application.fetch_env(:verk, :redis_url)
                      |> elem(1)
                      |> Redix.start_link([name: Verk.Redis])
    on_exit fn -> Redix.stop(redis) end
    { :ok, %{ redis: redis } }
  end

  describe "lpop_rpush_src_dest" do
    test "lpop from source rpush to dest", %{ redis: redis } do
      { :ok, _ } = Redix.command(redis, ~w(DEL source dest))
      { :ok, _ } = Redix.command(redis, ~w(RPUSH dest job1 job2 job3))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH source job4 job5 job6))

      assert Redix.command(redis, ["EVAL", @lpop_rpush_src_dest_script, 2, "source", "dest"]) == {:ok, 3 }

      assert Redix.command(redis, ~w(LRANGE source 0 -1)) == { :ok, [] }
      assert Redix.command(redis, ~w(LRANGE dest 0 -1)) == { :ok, ~w(job1 job2 job3 job6 job5 job4) }
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
      job = "{\"jid\":\"123\",\"queue\":\"test_queue\"}"
      other_job = "{\"jid\":\"456\",\"queue\":\"test_queue\"}"
      enqueued_job = "{\"jid\":\"789\",\"queue\":\"test_queue\"}"

      { :ok, _ } = Redix.command(redis, ~w(DEL retry queue:test_queue))
      { :ok, _ } = Redix.command(redis, ~w(ZADD retry 42 #{job} 45 #{other_job}))
      { :ok, _ } = Redix.command(redis, ~w(LPUSH queue:test_queue #{enqueued_job}))

      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "41"]) == {:ok, nil }
      assert Redix.command(redis, ["EVAL", @enqueue_retriable_job_script, 1, "retry", "42"]) == {:ok, job }

      assert Redix.command(redis, ~w(ZRANGEBYSCORE retry -inf +inf WITHSCORES)) == { :ok, [other_job, "45"] }
      assert Redix.command(redis, ~w(LRANGE queue:test_queue 0 -1)) == { :ok, [job, enqueued_job] }
    end
  end
end
