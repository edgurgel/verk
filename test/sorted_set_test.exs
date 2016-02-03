defmodule Verk.SortedSetTest do
  use ExUnit.Case
  import Verk.SortedSet

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

    assert count("retry") == 1
  end

  test "clear" do
    Redix.command!(Verk.Redis, ~w(ZADD retry 123 abc))

    assert clear("retry")

    assert Redix.command!(Verk.Redis, ~w(GET retry)) == nil
  end

  test "count with no items" do
    assert count("retry") == 0
  end

  test "range" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)
    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    assert range("retry") == [%{ job | original_json: json }]
  end

  test "range with no items" do
    assert range("retry") == []
  end

  test "delete_job having job with original_json" do
    job = %Verk.Job{class: "Class", args: []}
    json = Poison.encode!(job)

    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    job = %{ job | original_json: json}

    assert delete_job("retry", job) == true
  end

  test "delete_job with original_json" do
    json = %Verk.Job{class: "Class", args: []} |> Poison.encode!

    Redix.command!(Verk.Redis, ~w(ZADD retry 123 #{json}))

    assert delete_job("retry", json) == true
  end
end
