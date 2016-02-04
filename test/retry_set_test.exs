defmodule Verk.RetrySetTest do
  use ExUnit.Case
  import Verk.RetrySet
  import :meck

  setup do
    on_exit fn -> :meck.unload end
    :ok
  end

  test "count" do
    expect(Verk.SortedSet, :count, [key], 1)

    assert count == 1
  end

  test "clear" do
    expect(Verk.SortedSet, :clear, [key], true)

    assert clear
  end

  test "range" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    expect(Verk.SortedSet, :range, [key, 0, -1], [%{ job | original_json: json }])

    assert range == [%{ job | original_json: json }]
  end

  test "range with start and stop" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    expect(Verk.SortedSet, :range, [key, 1, 2], [%{ job | original_json: json }])

    assert range(1, 2) == [%{ job | original_json: json }]
  end

  test "delete_job having job with original_json" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    job = %{ job | original_json: json }

    expect(Verk.SortedSet, :delete_job, [key, json], true)

    assert delete_job(job) == true
  end

  test "delete_job with original_json" do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!
    expect(Verk.SortedSet, :delete_job, [key, json], true)

    assert delete_job(json) == true
  end
end
