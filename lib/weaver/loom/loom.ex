defmodule Weaver.Loom do
  @moduledoc """
  Enables running a topology of concurrent Weaver workers using `GenStage`.

  ## Supervisor

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

  ## Usage

  The easiest way to use Loom is to call `Weaver.Loom.weave/3` with:

    * a GraphQL query (String)
    * an optional list of options (see `Weaver.prepare/2`)
    * a callback function

  ### Callback function

  The callback function is called with:

    * a `result` tuple (`Weaver.Step.Result`)
    * a map of `assigns`

  It may return either of:

    * `{:ok, dispatch, next, dispatch_assigns, next_assigns}` to signal Loom to continue the stream
      * `dispatch` is a list of steps to be dispatched to the next level of workers - usually the (modified) result's `dispatched` list
      * `next` is a step to be processed next by the same worker - usually the result's `next` step
      * `dispatch_assigns` is a map to be passed to callbacks in the `dispatch` steps
      * `next_assigns` is a map to be passed to callbacks in the `next` step
    * `{:retry, assigns, delay}` to signal Loom to retry the step after the given delay (in milliseconds)
    * `{:error, error}` to signal Loom to stop processing this stream

  It can choose based on the `errors` it receives as an argument.

  ### Error handling

  Error cases outside Weaver:

    * an error in the resolver that can be retried (e.g. connection timeout)
      -> resolver adds retry with delay hint (in milliseconds) to `errors`
         e.g. `{:retry, reason, delay}`
    * an error in the resolver that can not be retried (e.g. wrong type returned)
      -> resolver adds error to `errors`
         e.g. `{:error, reason}`
    * an error in the callback function
      -> the callback is responsible for handling its errors
      -> Loom will catch uncaught errors, ignore them, and continue with the next event
  """

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
