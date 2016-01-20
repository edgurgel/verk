defmodule Verk.RetryTest do
  use ExUnit.Case
  import Verk.RetrySet

  setup do
    { :ok, redis } = Application.fetch_env(:verk, :redis_url)
                       |> elem(1)
                       |> Redix.start_link
    Redix.command!(redis, ~w(DEL retry))
    { :ok, %{ redis: redis } }
  end

  test "count", %{ redis: redis } do
    Redix.command!(redis, ~w(ZADD retry 123 abc))

    assert count(redis) == 1
  end

  test "count with no items", %{ redis: redis } do
    assert count(redis) == 0
  end

  test "range", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    Redix.command!(redis, ~w(ZADD retry 123 #{Poison.encode!(job)}))

    assert range(redis) == [job]
  end

  test "range with no items", %{ redis: redis } do
    assert range(redis) == []
  end
end
