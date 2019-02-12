defmodule Verk.ScheduleManagerTest do
  use ExUnit.Case, async: true
  import Verk.ScheduleManager
  import Mimic
  alias Verk.{ScheduleManager.State}

  setup :verify_on_exit!

  @now Verk.Time.now()
  @unix_now DateTime.to_unix(@now)

  setup do
    Application.put_env(:verk, :poll_interval, 50)
    stub(Verk.Time, :now, fn -> @now end)
    script = Verk.Scripts.sha("enqueue_retriable_job")
    {:ok, %{script: script}}
  end

  test "init load scripts and schedule fetch" do
    state = %State{redis: :redis}

    redis_url = Confex.get_env(:verk, :redis_url)

    expect(Redix, :start_link, fn ^redis_url -> {:ok, :redis} end)
    expect(Verk.Scripts, :load, fn :redis -> :ok end)

    assert init(:args) == {:ok, state}
    assert_receive :fetch_scheduled
  end

  test "handle_info :fetch_retryable without jobs to retry", %{script: script} do
    state = %State{redis: :redis}

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "retry", @unix_now] ->
      {:ok, nil}
    end)

    assert handle_info(:fetch_retryable, state) == {:noreply, state}
    assert_receive :fetch_retryable
  end

  test "handle_info :fetch_retryable with jobs to retry", %{script: script} do
    state = %State{redis: :redis}
    encoded_job = "encoded_job"

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "retry", @unix_now] ->
      {:ok, encoded_job}
    end)

    assert handle_info(:fetch_retryable, state) == {:noreply, state}
    assert_receive :fetch_retryable, 50
  end

  test "handle_info :fetch_retryable with jobs to retry and redis failed to apply the script", %{
    script: script
  } do
    state = %State{redis: :redis}

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "retry", @unix_now] ->
      {:error, %Redix.Error{message: "a message"}}
    end)

    assert handle_info(:fetch_retryable, state) == {:stop, :redis_failed, state}
  end

  test "handle_info :fetch_scheduled without jobs to run", %{script: script} do
    state = %State{redis: :redis}

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "schedule", @unix_now] ->
      {:ok, nil}
    end)

    assert handle_info(:fetch_scheduled, state) == {:noreply, state}
    assert_receive :fetch_scheduled
  end

  test "handle_info :fetch_scheduled with jobs to run", %{script: script} do
    state = %State{redis: :redis}
    encoded_job = "encoded_job"

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "schedule", @unix_now] ->
      {:ok, encoded_job}
    end)

    assert handle_info(:fetch_scheduled, state) == {:noreply, state}
    assert_receive :fetch_scheduled
  end

  test "handle_info :fetch_scheduled with jobs to run and redis failed to apply the script", %{
    script: script
  } do
    state = %State{redis: :redis}

    expect(Redix, :command, fn :redis, ["EVALSHA", ^script, 1, "schedule", @unix_now] ->
      {:error, %Redix.Error{message: "a message"}}
    end)

    assert handle_info(:fetch_scheduled, state) == {:stop, :redis_failed, state}
  end
end
