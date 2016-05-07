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

    assert count("sorted", redis) == {:ok, 1}
  end

  test "count with no items", %{ redis: redis } do
    assert count("sorted", redis) == {:ok, 0}
  end

  test "count!", %{ redis: redis } do
    assert count!("sorted", redis) == 0
  end

  test "clear", %{ redis: redis } do
    assert clear("sorted", redis) == {:ok, false}

    Redix.command!(redis, ~w(ZADD sorted 123 abc))
    assert clear("sorted", redis) == {:ok, true}

    assert Redix.command!(redis, ~w(GET sorted)) == nil
  end

  test "clear!", %{ redis: redis } do
    assert clear!("sorted", redis) == false

    Redix.command!(redis, ~w(ZADD sorted 123 abc))

    assert clear!("sorted", redis) == true

    assert Redix.command!(redis, ~w(GET sorted)) == nil
  end

  test "range", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert range("sorted", redis) == {:ok, [%{ job | original_json: json }]}
  end

  test "range with no items", %{ redis: redis } do
    assert range("sorted", redis) == {:ok, []}
  end

  test "range!", %{ redis: redis } do
    assert range!("sorted", redis) == []
  end

  test "delete_job with job", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    job = %{ job | original_json: json}

    assert delete_job("sorted", job, redis) == {:ok, false}

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert delete_job("sorted", job, redis) == {:ok, true}
  end

  test "delete_job! with job", %{ redis: redis } do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    job = %{ job | original_json: json}

    assert delete_job!("sorted", job, redis) == false

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert delete_job!("sorted", job, redis) == true
  end

  test "delete_job with original_json", %{ redis: redis } do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    assert delete_job("sorted", json, redis) == {:ok, false}

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert delete_job("sorted", json, redis) == {:ok, true}
  end

  test "delete_job! with original_json", %{ redis: redis } do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    assert delete_job!("sorted", json, redis) == false

    Redix.command!(redis, ~w(ZADD sorted 123 #{json}))

    assert delete_job!("sorted", json, redis) == true
  end
end
