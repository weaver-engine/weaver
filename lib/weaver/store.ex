defmodule Weaver.Store do
  @moduledoc """
  Interface for modules storing edges and markers.
  """

  alias Weaver.{Marker, Ref}

  # write
  @callback store(list(tuple()), list(tuple())) :: :ok | {:error, any()}

  # read
  @callback count(Ref.t(), String.t()) :: {:ok, integer()} | {:error, any()}
  @callback markers(Ref.t(), String.t(), keyword()) :: {:ok, list(Marker.t())} | {:error, any()}
  # @callback data(Ref.t(), String.t(), Marker.t() | nil) ::
  #             {:ok, list(tuple())} | {:error, any()}

  # operational
  @callback reset() :: :ok | {:error, any()}
end
