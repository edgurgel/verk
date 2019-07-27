defmodule Verk.Redis do
  @moduledoc """
  Module to interface a pool of Redis connections through Redix processes
  """

  def child_spec(_) do
    redis_url = Confex.fetch_env!(:verk, :redis_url)

    children =
      0..pool_size()
      |> Enum.map(fn i ->
        Supervisor.child_spec({Redix, {redis_url, [name: redis_name(i)]}}, id: {Redix, i})
      end)

    verk_redis_main = Supervisor.child_spec({Redix, {redis_url, [name: Verk.Redis]}}, id: Redix)

    Supervisor.Spec.supervisor(Supervisor, [
      [verk_redis_main | children],
      [strategy: :one_for_one]
    ])
  end

  def random() do
    number = :rand.uniform(pool_size()) - 1
    redis_name(number)
  end

  defp pool_size(), do: Confex.get_env(:verk, :redis_pool_size, 25)

  defp redis_name(number), do: :"Verk.Redis_#{number}"
end
