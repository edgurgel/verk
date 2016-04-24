defmodule Verk.Dsl do
  @moduledoc false

  @doc """
  Creates bang version of given function
  """
  defmacro bangify(result) do
    quote do
      case unquote(result) do
        :ok -> nil
        {:ok, value} -> value
        {:error, error} -> raise error
      end
    end
  end
end
