defmodule Verk.SortedSetTest do
  use ExUnit.Case
  import Verk.SortedSet

  setup do
    { :ok, redis } = Application.fetch_env!(:verk, :redis_url) |> Redix.start_link
    Redix.command!(redis, ~w(DEL sorted))
    { :ok, %{ redis: redis } }
  end

  test "count", %{ redis: redis } do
    Redix.command!(redis, ~w(ZADD sorted 123 abc))

    assert count("sorted", redis) == 1
  end

  test "clear", %{ redis: redis } do
    Redix.command!(redis, ~w(ZADD sorted 123 abc))

    assert clear("sorted", redis)

    assert Redix.command!(redis, ~w(GET sorted)) == nil
  end

  test "count with no items", %{ redis: redis } do
    assert count("sorted", redis) == 0
  end

  test "range", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert range("sorted", redis) == [%{ job | original_json: json }]
  end

  test "range with no items", %{ redis: redis } do
    assert range("sorted", redis) == []
  end

  test "delete_job having job with original_json", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    job = %{ job | original_json: json}

    assert delete_job("sorted", job, redis) == true
  end

  test "delete_job with original_json", %{ redis: redis } do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert delete_job("sorted", json, redis) == true
  end
end
