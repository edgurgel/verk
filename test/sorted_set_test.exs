defmodule Verk.SortedSetTest do
  use ExUnit.Case
  import Verk.SortedSet
  import :meck

  @requeue_now_script Verk.Scripts.sha("requeue_job_now")

  setup do
    on_exit fn -> unload() end
    { :ok, redis } = Confex.get_env(:verk, :redis_url) |> Redix.start_link
    Redix.command!(redis, ~w(DEL sorted))
    { :ok, %{ redis: redis } }
  end

  describe "count/2" do
    test "with items", %{ redis: redis } do
      Redix.command!(redis, ~w(ZADD sorted 123 abc))

      assert count("sorted", redis) == {:ok, 1}
    end

    test "with no items", %{ redis: redis } do
      assert count("sorted", redis) == {:ok, 0}
    end
  end

  describe "count!/2" do
    test "with no items", %{ redis: redis } do
      assert count!("sorted", redis) == 0
    end

    test "with items", %{ redis: redis } do
      Redix.command!(redis, ~w(ZADD sorted 123 abc))

      assert count!("sorted", redis) == 1
    end
  end

  describe "clear/2" do
    test "with no items", %{ redis: redis } do
      assert clear("sorted", redis) == {:ok, false}
    end

    test "with items", %{ redis: redis } do
      Redix.command!(redis, ~w(ZADD sorted 123 abc))
      assert clear("sorted", redis) == {:ok, true}

      assert Redix.command!(redis, ~w(GET sorted)) == nil
    end
  end

  describe "clear!/2" do
    test "with no items", %{ redis: redis } do
      assert clear!("sorted", redis) == false
    end

    test "with items", %{ redis: redis } do
      Redix.command!(redis, ~w(ZADD sorted 123 abc))
      assert clear!("sorted", redis) == true

      assert Redix.command!(redis, ~w(GET sorted)) == nil
    end
  end

  describe "range/2" do
    test "with items", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert range("sorted", redis) == {:ok, [%{ job | original_json: json }]}
    end

    test "with no items", %{ redis: redis } do
      assert range("sorted", redis) == {:ok, []}
    end
  end

  describe "range!/2" do
    test "with items", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert range!("sorted", redis) == [%{ job | original_json: json }]
    end

    test "with no items", %{ redis: redis } do
      assert range!("sorted", redis) == []
    end
  end

  describe "range_with_score/2" do
    test "with items", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert range_with_score("sorted", redis) == {:ok, [{%{ job | original_json: json }, 123}]}
    end

    test "with no items", %{ redis: redis } do
      assert range_with_score("sorted", redis) == {:ok, []}
    end
  end

  describe "range_with_score!/2" do
    test "with items", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert range_with_score("sorted", redis) == {:ok, [{%{ job | original_json: json }, 123}]}
    end

    test "with no items", %{ redis: redis } do
      assert range_with_score!("sorted", redis) == []
    end
  end

  describe "delete_job/3" do
    test "with job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json}

      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert delete_job("sorted", job, redis) == {:ok, true}
    end

    test "with no job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}

      assert delete_job("sorted", job, redis) == {:ok, false}
    end

    test "with original_json", %{ redis: redis } do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

      add_job!(json, redis)

      assert delete_job("sorted", json, redis) == {:ok, true}
    end
  end

  describe "delete_job!/3" do
    test "with job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json}

      Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

      assert delete_job!("sorted", job, redis) == true
    end

    test "with no job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", args: []}

      assert delete_job!("sorted", job, redis) == false
    end

    test "with original_json", %{ redis: redis } do
      json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

      add_job!(json, redis)

      assert delete_job!("sorted", json, redis) == true
    end
  end

  describe "requeue_job/3" do
    test "with no job in original queue", %{ redis: redis } do
      json = %Verk.Job{class: "Class", queue: :default, args: []} |> Poison.encode!

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, nil })

      assert requeue_job("sorted", json, redis) == {:ok, false}
      assert validate Redix
    end

    test "with job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", queue: :default, args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json}

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, "job" })

      assert requeue_job("sorted", job, redis) == {:ok, true}
      assert validate Redix
    end

    test "with original json", %{ redis: redis } do
      json = %Verk.Job{class: "Class", queue: :default, args: []} |> Poison.encode!

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, "job" })

      assert requeue_job("sorted", json, redis) == {:ok, true}
      assert validate Redix
    end
  end

  describe "requeue_job!/3" do
    test "with no job in original queue", %{ redis: redis } do
      json = %Verk.Job{class: "Class", queue: :default, args: []} |> Poison.encode!

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, nil })

      assert requeue_job!("sorted", json, redis) == false
      assert validate Redix
    end

    test "with job", %{ redis: redis } do
      job = %Verk.Job{class: "Class", queue: :default, args: []}
      json = Poison.encode!(job)
      job = %{ job | original_json: json}

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, "job" })

      assert requeue_job!("sorted", job, redis) == true
      assert validate Redix
    end

    test "with original json", %{ redis: redis } do
      json = %Verk.Job{class: "Class", queue: :default, args: []} |> Poison.encode!

      expect(Redix, :command, [redis, ["EVALSHA", @requeue_now_script, 1, "sorted", json]], { :ok, "job" })

      assert requeue_job!("sorted", json, redis) == true
      assert validate Redix
    end
  end

  defp add_job!(json, redis), do: Redix.command!(redis, ~w(ZADD sorted 123 #{json}))
end
