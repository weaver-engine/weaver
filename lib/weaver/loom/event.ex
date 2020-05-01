defmodule Weaver.Loom.Event do
  @moduledoc """
  Wraps a `Weaver.Step` together with Loom-specific data.
  """

  @enforce_keys [:callback, :step]
  defstruct @enforce_keys ++ [dispatch_assigns: %{}, next_assigns: %{}]

  @type(callback_args() :: Weaver.Step.Result.t(), map())

  @type callback_return_ok() :: {:ok, Weaver.Step.Result.t(), map()}
  @type callback_return_retry() :: {:retry, map(), non_neg_integer()}
  @type callback_return_error() :: {:error, any()}
  @type callback_return() ::
          callback_return_ok() | callback_return_retry() | callback_return_error()

  @type t() :: %__MODULE__{
          callback: (callback_args() -> callback_return()),
          step: Weaver.Step.t(),
          dispatch_assigns: map(),
          next_assigns: map()
        }

  alias Weaver.Step.Result
  alias Weaver.Util

  @doc """
  Processes a step, calls the callback and handles the result.
  """
  @spec process(__MODULE__.t()) ::
          {:ok, list(), Weaver.Step.t() | nil}
          | {:error, any()}
          | {:retry, __MODULE__.t(), non_neg_integer()}
  def process(event) do
    event.step
    |> Weaver.Step.process()
    |> safe_callback(event)
    |> case do
      {:ok, result, dispatch_assigns, next_assigns} ->
        dispatched =
          Result.dispatched(result)
          |> Enum.map(fn step ->
            event
            |> Map.put(:step, step)
            |> Map.put(:next_assings, %{})
            |> Map.update!(:dispatch_assigns, &Util.Map.merge_non_nil(&1, dispatch_assigns))
          end)

        next =
          case Result.next(result) do
            nil ->
              nil

            step ->
              event
              |> Map.put(:step, step)
              |> Map.update!(:next_assigns, &Util.Map.merge_non_nil(&1, next_assigns))
          end

        {:ok, dispatched, next}

      {:retry, assigns, delay} ->
        retry_event = %{event | assigns: assigns}

        {:retry, retry_event, delay}

      {:error, e} ->
        {:error, e}
    end
  end

  def safe_callback(result, %{
        callback: callback,
        dispatch_assigns: dispatch_assigns,
        next_assigns: next_assigns
      })
      when is_function(callback, 3) and is_map(dispatch_assigns) and is_map(next_assigns) do
    callback.(result, dispatch_assigns, next_assigns)
  rescue
    e ->
      {:error, e}
  end
end
