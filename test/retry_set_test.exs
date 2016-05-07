defmodule Verk.RetrySetTest do
  use ExUnit.Case
  alias Verk.SortedSet
  import Verk.RetrySet
  import :meck

  setup do
    new SortedSet
    on_exit fn -> :meck.unload end
    :ok
  end

  test "add" do
    job = %Verk.Job{ retry_count: 1 }
    failed_at = 1
    retry_at  = "45.0"
    expect(Poison, :encode!, [job], :payload)
    expect(Redix, :command, [:redis, ["ZADD", "retry", retry_at, :payload]], { :ok, 1 })

    assert add(job, failed_at, :redis) == :ok

    assert validate [Poison, Redix]
  end

  test "add!" do
    job = %Verk.Job{ retry_count: 1 }
    failed_at = 1
    retry_at  = "45.0"
    expect(Poison, :encode!, [job], :payload)
    expect(Redix, :command, [:redis, ["ZADD", "retry", retry_at, :payload]], { :ok, 1 })

    assert add!(job, failed_at, :redis) == nil

    assert validate [Poison, Redix]
  end

  test "count" do
    expect(SortedSet, :count, [key, Verk.Redis], {:ok, 1})

    assert count == {:ok, 1}
    assert validate SortedSet
  end

  test "count!" do
    expect(SortedSet, :count!, [key, Verk.Redis], 1)

    assert count! == 1
    assert validate SortedSet
  end

  test "clear" do
    expect(SortedSet, :clear, [key, Verk.Redis], {:ok, false})

    assert clear == {:ok, false}
    assert validate SortedSet
  end

  test "clear!" do
    expect(SortedSet, :clear!, [key, Verk.Redis], true)

    assert clear! == true
    assert validate SortedSet
  end

  test "range" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    expect(SortedSet, :range, [key, 0, -1, Verk.Redis], {:ok, [%{ job | original_json: json }]})

    assert range == {:ok, [%{ job | original_json: json }]}
    assert validate SortedSet
  end

  test "range with start and stop" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    expect(SortedSet, :range, [key, 1, 2, Verk.Redis], {:ok, [%{ job | original_json: json }]})

    assert range(1, 2) == {:ok, [%{ job | original_json: json }]}
    assert validate SortedSet
  end

  test "range!" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    expect(SortedSet, :range!, [key, 0, -1, Verk.Redis], [%{ job | original_json: json }])

    assert range! == [%{ job | original_json: json }]
    assert validate SortedSet
  end

  test "delete_job having job with original_json" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    job = %{ job | original_json: json }

    expect(SortedSet, :delete_job, [key, json, Verk.Redis], :ok)

    assert delete_job(job) == :ok
    assert validate SortedSet
  end

  test "delete_job with original_json" do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!
    expect(SortedSet, :delete_job, [key, json, Verk.Redis], {:ok, true})

    assert delete_job(json) == {:ok, true}
    assert validate SortedSet
  end

  test "delete_job! having job with original_json" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    job = %{ job | original_json: json }

    expect(SortedSet, :delete_job!, [key, json, Verk.Redis], true)

    assert delete_job!(job) == true
    assert validate SortedSet
  end

  test "delete_job! with original_json" do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!
    expect(SortedSet, :delete_job!, [key, json, Verk.Redis], true)

    assert delete_job!(json) == true
    assert validate SortedSet
  end
end
