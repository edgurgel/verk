defmodule Verk.LogTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  test "logs done with time in milliseconds" do
    worker = self
    job = %Verk.Job{}
    start_time = Timex.DateTime.now

    assert capture_log(fn ->
      Verk.Log.done(job, start_time, worker)
    end) =~ ~r/done: \d+ ms/
  end

  test "logs done with time in seconds" do
    worker = self
    job = %Verk.Job{}
    start_time = Timex.DateTime.now
    |> Timex.shift(seconds: -2)

    assert capture_log(fn ->
      Verk.Log.done(job, start_time, worker)
    end) =~ ~r/done: \d+ s/
  end

  test "logs fail with time in milliseconds" do
    worker = self
    job = %Verk.Job{}
    start_time = Timex.DateTime.now

    assert capture_log(fn ->
      Verk.Log.fail(job, start_time, worker)
    end) =~ ~r/fail: \d+ ms/
  end

  test "logs fail with time in seconds" do
    worker = self
    job = %Verk.Job{}
    start_time = Timex.DateTime.now
    |> Timex.shift(seconds: -2)

    assert capture_log(fn ->
      Verk.Log.fail(job, start_time, worker)
    end) =~ ~r/fail: \d+ s/
  end
end
