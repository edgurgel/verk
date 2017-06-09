defmodule Verk.LogTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  alias Verk.Time

  describe "start/2" do
    test "logs start" do
      worker = self()
      job = %Verk.Job{}

      assert capture_log(fn -> Verk.Log.start(job, worker) end) =~ ~r/start/
    end
  end

  describe "done/3" do
    test "logs done with time in milliseconds" do
      worker = self()
      job = %Verk.Job{}
      start_time = Verk.Time.now

      assert capture_log(fn ->
        Verk.Log.done(job, start_time, worker)
      end) =~ ~r/done: \d+ ms/
    end

    test "logs done with time in seconds" do
      worker = self()
      job = %Verk.Job{}
      start_time = Verk.Time.now
      |> Verk.Time.shift(-2)

      assert capture_log(fn ->
        Verk.Log.done(job, start_time, worker)
      end) =~ ~r/done: \d+ s/
    end
  end

  describe "fail/3" do
    test "logs fail with time in milliseconds" do
      worker = self()
      job = %Verk.Job{}
      start_time = Verk.Time.now

      assert capture_log(fn ->
        Verk.Log.fail(job, start_time, worker)
      end) =~ ~r/fail: \d+ ms/
    end

    test "logs fail with time in seconds" do
      worker = self()
      job = %Verk.Job{}
      start_time = Verk.Time.now
      |> Time.shift(-2, :seconds)

      assert capture_log(fn ->
        Verk.Log.fail(job, start_time, worker)
      end) =~ ~r/fail: \d+ s/
    end
  end
end
