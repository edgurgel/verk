defmodule Verk.ScheduleManagerTest do
  use ExUnit.Case
  import Verk.ScheduleManager
  import :meck
  alias Verk.ScheduleManager.State

  setup do
    Application.put_env(:verk, :poll_interval, 50)
    script = Verk.Scripts.sha("enqueue_retriable_job")
    on_exit fn -> unload end
    { :ok, %{ script: script } }
  end

  test "init load scripts and schedule fetch" do
    state = %State{ redis: :redis }

    { :ok, redis_url } = Application.fetch_env(:verk, :redis_url)

    expect(Redix, :start_link, [redis_url], {:ok, :redis })
    expect(Verk.Scripts, :load, [:redis], :ok)

    assert init(:args) == { :ok, state }
    assert_receive :fetch_scheduled

    assert validate [Redix, Verk.Scripts]
  end

  test "handle_info :fetch_retryable without jobs to retry", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "retry", now]], {:ok, nil})

    assert handle_info(:fetch_retryable, state) == { :noreply, state }
    assert_receive :fetch_retryable

    assert validate [Timex.Time, Redix]
  end

  test "handle_info :fetch_retryable with jobs to retry", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    encoded_job = "encoded_job"
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "retry", now]], {:ok, encoded_job})

    assert handle_info(:fetch_retryable, state) == { :noreply, state }
    assert_receive :fetch_retryable, 5

    assert validate [Timex.Time, Redix]
  end

  test "handle_info :fetch_retryable with jobs to retry and redis failed to apply the script", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "retry", now]], {:error, %Redix.Error{message: "a message"}})

    assert handle_info(:fetch_retryable, state) == { :stop, :redis_failed, state }

    assert validate [Timex.Time, Redix]
  end

  test "handle_info :fetch_scheduled without jobs to run", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "schedule", now]], {:ok, nil})

    assert handle_info(:fetch_scheduled, state) == { :noreply, state }
    assert_receive :fetch_scheduled

    assert validate [Timex.Time, Redix]
  end

  test "handle_info :fetch_scheduled with jobs to run", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    encoded_job = "encoded_job"
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "schedule", now]], {:ok, encoded_job})

    assert handle_info(:fetch_scheduled, state) == { :noreply, state }
    assert_receive :fetch_scheduled

    assert validate [Timex.Time, Redix]
  end

  test "handle_info :fetch_scheduled with jobs to run and redis failed to apply the script", %{ script: script } do
    state = %State{ redis: :redis }
    now = :now
    expect(Timex.Time, :now, [:seconds], now)
    expect(Redix, :command, [:redis, ["EVALSHA", script, 1, "schedule", now]], {:error, %Redix.Error{message: "a message"}})

    assert handle_info(:fetch_scheduled, state) == { :stop, :redis_failed, state }

    assert validate [Timex.Time, Redix]
  end
end
