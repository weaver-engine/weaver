defmodule Weaver.Store do
  @moduledoc """
  Interface for modules storing edges and cursors.
  """

  alias Weaver.{Cursor, Ref}

  # write
  @callback store(list(tuple()), list(tuple())) :: :ok | {:error, any()}

  # read
  @callback count(Ref.t(), String.t()) :: {:ok, integer()} | {:error, any()}
  @callback cursors(Ref.t(), String.t(), keyword()) :: {:ok, list(Cursor.t())} | {:error, any()}
  # @callback data(Ref.t(), String.t(), Cursor.t() | nil) ::
  #             {:ok, list(tuple())} | {:error, any()}

  # operational
  @callback reset() :: :ok | {:error, any()}
end
