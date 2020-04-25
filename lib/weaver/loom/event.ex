defmodule Weaver.Loom.Event do
  @moduledoc """
  Wraps a `Weaver.Step` together with Loom-specific data.
  """

  @enforce_keys [:callback, :step]
  defstruct @enforce_keys ++ [assigns: %{}]

  @type(callback_args_ok() :: Weaver.Step.Result.t(), Weaver.Step.t(), map())
  @type(callback_args_error() :: {:error, any()}, Weaver.Step.t(), map())
  @type callback_args() :: callback_args_ok() | callback_args_error()

  @type callback_return_ok() :: {:ok, Weaver.Step.Result.t(), map()} | {:error, any()}
  @type callback_return_error() :: {:retry, map()} | :ok
  @type callback_return() :: callback_return_ok() | callback_return_error()

  @type t() :: %__MODULE__{
          callback: (callback_args() -> callback_return()),
          step: Weaver.Step.t(),
          assigns: map()
        }

  @doc "Processes a step, calls the callback and handles its result."
  @spec process(__MODULE__.t()) ::
          {:ok, list(), Weaver.Step.t() | nil}
          | {:error, any()}
          | {:retry, map(), non_neg_integer()}
  def process(event) do
    try do
      result = Weaver.Step.process(event.step)

      case event.callback.(result, event.assigns) do
        {:ok, {_data, _meta, dispatched, next}, assigns} ->
          dispatched =
            Enum.map(dispatched, fn step ->
              %{event | step: step, assigns: assigns}
            end)

          next = if next, do: %{event | step: next, assigns: assigns}

          {:ok, dispatched, next}

        {:error, e} ->
          {:error, e}
      end
    rescue
      e in ExTwitter.RateLimitExceededError ->
        {:retry, event, :timer.seconds(e.reset_in)}

      _e in Dlex.Error ->
        {:retry, event, :timer.seconds(5)}

      e ->
        case event.callback.({:error, e}, event.assigns) do
          {:retry, assigns, interval} ->
            retry_event = %{event | assigns: assigns}

            {:retry, retry_event, interval}

          :ok ->
            {:error, e}
        end
    end
  end
end
