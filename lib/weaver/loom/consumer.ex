defmodule Weaver.Loom.Consumer do
  @moduledoc """
  Represents a worker at the bottom `GenStage` level.
  Dispatched steps are processed recursively via `Weaver.Step.process/1`.
  A step is passed again to `Weaver.Step.process/1` as long as it
  returns a continuation (`Weaver.Step` with a `:cursor`).
  Otherwise, it sends demand to the `GenStage` level above.

  Implements a `GenStage` `consumer`.
  """

  use GenStage

  @max_demand 1

  def start_link(opts = {name, _subscriptions}) do
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @impl GenStage
  def init({name, subscriptions}) do
    subscriptions =
      Enum.map(subscriptions, fn
        {name, opts} -> {name, Keyword.put_new(opts, :max_demand, @max_demand)}
        name -> {name, max_demand: @max_demand}
      end)

    {:consumer, %{name: name}, subscribe_to: subscriptions}
  end

  @impl GenStage
  def handle_events(events, _from, state) do
    :ok = handle_remaining(events)
    {:noreply, [], state}
  end

  defp handle_remaining([event | events]) do
    {data, _meta, dispatched, next} = Weaver.Step.process(event)
    Weaver.Graph.store!(data)
    handle_remaining(dispatched ++ List.wrap(next) ++ events)
  end

  defp handle_remaining([]) do
    :ok
  end
end
