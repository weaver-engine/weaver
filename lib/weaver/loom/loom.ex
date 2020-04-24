defmodule Weaver.Loom do
  @moduledoc """
  Enables running a topology of concurrent Weaver workers using `GenStage`.

  ## Usage

  `Weaver.Loom` implements the `Supervisor` specification, so you can run it
  as part of any supervision tree:

  ```
  defmodule MyApp.Application do
    ...

    def start(_type, _args) do
      children = [
        ...

        Weaver.Loom
      ]

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]
      Supervisor.start_link(children, opts)
    end
  end
  ```
  """

  defmodule Event do
    @moduledoc """
    Wraps a `Weaver.Step` together with Loom-specific data.
    """

    @enforce_keys [:callback, :step]
    defstruct @enforce_keys ++ [assigns: %{}]

    @type(callback_args_ok() :: Weaver.Step.Result.t(), map())
    @type(callback_args_error() :: {:error, any()}, map())
    @type callback_args() :: callback_args_ok() | callback_args_error()

    @type callback_return_ok() :: {:ok, Weaver.Step.Result.t(), map()} | {:error, any()}
    @type callback_return_error() :: {:retry, map()} | :ok
    @type callback_return() :: callback_return_ok() | callback_return_error()

    @type t() :: %__MODULE__{
            callback: (callback_args() -> callback_return()),
            step: Weaver.Step.t(),
            assigns: map()
          }
  end

  use Supervisor

  alias Weaver.Loom.{Consumer, Event, Producer, Prosumer}

  def start_link(_arg) do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def prepare(query, opts \\ [], callback)

  def prepare(query, opts, callback) when is_binary(query) do
    Weaver.prepare(query, opts)
    |> prepare(opts, callback)
  end

  def prepare(step = %Weaver.Step{}, _opts, callback) do
    %Event{step: step, callback: callback}
  end

  def weave(query, opts \\ [], callback)

  def weave(query, opts, callback) when is_binary(query) do
    prepare(query, opts, callback)
    |> weave()
  end

  def weave(step = %Weaver.Step{}, opts, callback) do
    prepare(step, opts, callback)
    |> weave()
  end

  def weave(event = %Event{}) do
    Producer.add(event)
  end

  def init(:ok) do
    children = [
      Producer,
      processor(:weaver_processor_1a, [Producer]),
      processor(:weaver_processor_2a, [:weaver_processor_1a]),
      processor(:weaver_processor_3a, [:weaver_processor_2a]),
      processor(:weaver_processor_4a, [:weaver_processor_3a]),
      processor(:weaver_processor_5a, [:weaver_processor_4a]),
      processor(:weaver_processor_6a, [:weaver_processor_5a]),
      processor(:weaver_processor_7a, [:weaver_processor_6a]),
      processor(:weaver_processor_8a, [:weaver_processor_7a]),
      processor(:weaver_processor_9a, [:weaver_processor_8a], Consumer)
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp processor(name, subscriptions, role \\ Prosumer) do
    Supervisor.child_spec({role, {name, subscriptions}}, id: name)
  end
end
