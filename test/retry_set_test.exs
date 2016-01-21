defmodule Verk.RetryTest do
  use ExUnit.Case
  import Verk.RetrySet

  setup do
    { :ok, pid } = Application.fetch_env(:verk, :redis_url)
                       |> elem(1)
                       |> Redix.start_link([name: Verk.Redis])
    Redix.command!(pid, ~w(DEL retry))
    on_exit fn ->
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end
    :ok
  end

  test "count" do
    Redix.command!(Verk.Redis, ~w(ZADD retry 123 abc))

    assert count == 1
  end

  test "count with no items" do
    assert count == 0
  end

  test "range" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    assert range == [%{ job | original_json: json }]
  end

  test "range with no items" do
    assert range == []
  end

  test "delete_job having job with original_json" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    job = %{ job | original_json: json}

    assert delete_job(job) == true
  end

  test "delete_job with original_json" do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    assert delete_job(json) == true
  end
end
