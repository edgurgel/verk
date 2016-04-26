defmodule Verk.Scripts do
  @moduledoc """
  Provides helpers to load lua scripts into redis and calculate sha1
  """

  @spec load(pid) :: :ok
  def load(redis) do
    dir = :code.priv_dir(:verk)
    for file <- File.ls!(dir),
        path = Path.join(dir, file),
        script = File.read!(path) do
      Redix.command!(redis, ["SCRIPT", "LOAD", script])
    end
    :ok
  end

  @spec sha(binary) :: binary
  def sha(script_name) do
    script = Path.join(:code.priv_dir(:verk), "#{script_name}.lua")
    hash_sha = :crypto.hash(:sha, File.read!(script))
    Base.encode16(hash_sha, case: :lower)
  end
end
